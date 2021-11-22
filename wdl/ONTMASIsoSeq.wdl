version 1.0

######################################################################################
## A workflow that performs single sample variant calling on Oxford Nanopore reads
## from one or more flow cells. The workflow merges multiple samples into a single BAM
## prior to variant calling.
######################################################################################

import "tasks/ONTUtils.wdl" as ONT
import "tasks/Utils.wdl" as Utils
import "tasks/NanoPlot.wdl" as NP
import "tasks/AlignReads.wdl" as AR
import "tasks/Finalize.wdl" as FF

workflow ONTMASIsoSeq {
    input {
        String gcs_fastq_dir

        String model_name

        File ref_map_file
        String participant_name
        String prefix
        Int num_shards = 100

        String gcs_out_root_dir
    }

    parameter_meta {
        gcs_fastq_dir:       "GCS path to unaligned ONT fastq files"
        model_name:          "Longbow model name"
        ref_map_file:        "table indicating reference sequence and auxillary file locations"
        participant_name:    "name of the participant from whom these samples were obtained"
        prefix:              "prefix for output files"
        num_shards:          "number of shards to break the input data into and process in parallel"
        gcs_out_root_dir:    "GCS bucket to store the reads, variants, and metrics files"
    }

    Map[String, String] ref_map = read_map(ref_map_file)

    String outdir = sub(gcs_out_root_dir, "/$", "") + "/ONTMASIsoSeq/~{participant_name}"

    call Utils.ListFilesOfType { input: gcs_dir = gcs_fastq_dir, suffixes = [".fastq", ".fq", ".fastq.gz", ".fq.gz"] }

    Int lines_per_chunk = ceil(length(read_lines(ListFilesOfType.manifest))/num_shards)
    call Utils.ChunkManifest as PartitionFastqManifest { input: manifest = ListFilesOfType.manifest, manifest_lines_per_chunk = lines_per_chunk }

    String RG = "@RG\\tID:~{prefix}\\tSM:~{participant_name}"

    scatter (manifest_chunk in PartitionFastqManifest.manifest_chunks) {
        call Process {
            input:
                fastq_files = read_lines(manifest_chunk),
                model_name  = model_name,
                prefix      = prefix
        }

        call AR.Minimap2 as Align {
            input:
                reads      = [ Process.extracted ],
                ref_fasta  = ref_map['fasta'],
                RG         = RG,
                map_preset = "splice"
        }
    }

    call Utils.MergeBams as MergeUnfiltered { input: bams = Process.annotated_unfiltered }
    call Stats as StatsUnfiltered { input: bam = MergeUnfiltered.merged_bam, prefix = "~{prefix}.unfiltered" }

    call Utils.MergeBams as MergeFiltered { input: bams = Process.annotated_filtered }
    call Stats as StatsFiltered { input: bam = MergeFiltered.merged_bam, prefix = "~{prefix}.filtered" }

    call Utils.MergeBams as MergeAligned { input: bams = Align.aligned_bam }

    call NP.NanoPlotFromRichFastqs { input: fastqs = read_lines(ListFilesOfType.manifest) }
    call NP.NanoPlotFromBam { input: bam = MergeAligned.merged_bam, bai = MergeAligned.merged_bai }

    # Finalize data
    String adir = outdir + "/alignments"
    File bam = MergeAligned.merged_bam
    File bai = MergeAligned.merged_bai

    call FF.FinalizeToFile as FinalizeBam { input: outdir = adir, file = bam, name = "~{participant_name}.bam" }
    call FF.FinalizeToFile as FinalizeBai { input: outdir = adir, file = bai, name = "~{participant_name}.bam.bai" }

    String updir = outdir + "/stats/unfiltered/png"
    String usdir = outdir + "/stats/unfiltered/svg"
    call FF.FinalizeToDir as FinalizeStatsUnfilteredPng { input: outdir = updir, files = StatsUnfiltered.pngs }
    call FF.FinalizeToDir as FinalizeStatsUnfilteredSvg { input: outdir = usdir, files = StatsUnfiltered.svgs }

    String fpdir = outdir + "/stats/filtered/png"
    String fsdir = outdir + "/stats/filtered/svg"
    call FF.FinalizeToDir as FinalizeStatsFilteredPng { input: outdir = fpdir, files = StatsFiltered.pngs }
    call FF.FinalizeToDir as FinalizeStatsFilteredSvg { input: outdir = fsdir, files = StatsFiltered.svgs }

    call FF.FinalizeToFile as FinalizeNPRichFqStats { input: outdir = outdir + "/stats/nanoplot/fastq", file = NanoPlotFromRichFastqs.stats }
    call FF.FinalizeToDir as FinalizeNPRichFqPlots { input: outdir = outdir + "/stats/nanoplot/fastq", files = NanoPlotFromRichFastqs.plots }

    call FF.FinalizeToFile as FinalizeNPBamStats { input: outdir = outdir + "/stats/nanoplot/bam", file = NanoPlotFromBam.stats }
    call FF.FinalizeToDir as FinalizeNPBamPlots { input: outdir = outdir + "/stats/nanoplot/bam", files = NanoPlotFromBam.plots }

    output {
        File merged_bam = FinalizeBam.gcs_path
        File merged_bai = FinalizeBai.gcs_path

        String stats_unfiltered_pngs = FinalizeStatsUnfilteredPng.gcs_dir
        String stats_unfiltered_svgs = FinalizeStatsUnfilteredSvg.gcs_dir
        String stats_filtered_pngs = FinalizeStatsFilteredPng.gcs_dir
        String stats_filtered_svgs = FinalizeStatsFilteredSvg.gcs_dir

        File nanoplot_fq_stats = FinalizeNPRichFqStats.gcs_path
        File nanoplot_fq_dir = FinalizeNPRichFqPlots.gcs_dir

        File nanoplot_bam_stats = FinalizeNPBamStats.gcs_path
        File nanoplot_bam_dir = FinalizeNPBamPlots.gcs_dir
    }
}

task Process {
    input {
        Array[File] fastq_files
        String model_name
        String prefix

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 10 * ceil(size(fastq_files, "GB"))

    command <<<
        set -euxo pipefail

        DIR=$(dirname ~{fastq_files[0]})

        longbow convert $DIR | \
            longbow annotate -m ~{model_name} | \
                tee ~{prefix}.annotated_unfiltered.bam | \
            longbow filter | \
                tee ~{prefix}.annotated_filtered.bam | \
            longbow segment | \
            longbow extract -o ~{prefix}.extracted.bam
    >>>

    output {
        File annotated_unfiltered = "~{prefix}.annotated_unfiltered.bam"
        File annotated_filtered = "~{prefix}.annotated_filtered.bam"
        File extracted = "~{prefix}.extracted.bam"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          4,
        mem_gb:             4,
        disk_gb:            1,
        boot_disk_gb:       10,
        preemptible_tries:  1,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-longbow:0.4.7-kvg6"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task Stats {
    input {
        File bam
        String prefix

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 2 * ceil(size(bam, "GB"))

    command <<<
        set -euxo pipefail

        longbow stats -o ~{prefix}.stats ~{bam}
    >>>

    output {
        Array[File] pngs = glob("*.png")
        Array[File] svgs = glob("*.svg")
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             2,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  1,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-longbow:0.4.7-kvg6"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}
