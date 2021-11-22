version 1.0

##########################################################################################
## A workflow that performs CCS correction and IsoSeq processing on PacBio HiFi reads from
## a single flow cell. The workflow shards the subreads into clusters and performs CCS in
## parallel on each cluster.  Error-corrected reads are then processed with PacBio's
## IsoSeq software.  A number of metrics and figures are produced along the way.
##########################################################################################

import "tasks/PBUtils.wdl" as PB
import "tasks/Utils.wdl" as Utils
import "tasks/AlignReads.wdl" as AR
import "tasks/SQANTI.wdl" as SQANTI
import "tasks/Finalize.wdl" as FF

workflow PBCCSScIsoSeq {
    input {
        Array[File] ccs_bams
        Array[File] ccs_pbis

        File ref_map_file
        File ref_gtf
        String participant_name
        File barcode_file

        Boolean drop_per_base_N_pulse_tags = true

        String gcs_out_root_dir
    }

    parameter_meta {
        ccs_bams:         "GCS path to CCS BAM files"
        ccs_pbis:         "GCS path to CCS BAM .pbi indices"

        ref_map_file:     "table indicating reference sequence and auxillary file locations"
        participant_name: "name of the participant from whom these samples were obtained"
        barcode_file:     "GCS path to the fasta file that specifies the expected set of multiplexing barcodes"

        gcs_out_root_dir: "GCS bucket to store the corrected/uncorrected reads, variants, and metrics files"
    }

    Map[String, String] ref_map = read_map(ref_map_file)

    String outdir = sub(gcs_out_root_dir, "/$", "") + "/PBCCSScIsoSeq/~{participant_name}"

    # gather across (potential multiple) input CCS BAMs
    if (length(ccs_bams) > 1) {
        call Utils.MergeBams as MergeAllReads { input: bams = ccs_bams, prefix = participant_name }
        call PB.PBIndex as IndexCCSUnalignedReads { input: bam = MergeAllReads.merged_bam }
    }

    File bam = select_first([MergeAllReads.merged_bam, ccs_bams[0]])
    File pbi = select_first([IndexCCSUnalignedReads.pbi, ccs_pbis[0]])

    # select a small number of reads to process
    call Utils.SelectNReadsFromBam { input: bam = bam }

    # align raw reads
    call PB.Align as AlignSelectedReads {
        input:
            bam          = SelectNReadsFromBam.selected_bam,
            ref_fasta    = ref_map['fasta'],
            sample_name  = participant_name,
            drop_per_base_N_pulse_tags = true,
            map_preset   = "ISOSEQ",
            prefix       = participant_name,
    }

    # demultiplex BAM
    call PB.Demultiplex {
        input:
            bam = bam,
            prefix = participant_name,
            barcode_file = barcode_file,
            isoseq = true,
            dump_clips = false,
            split_bam_named = true
    }

    # make reports on demultiplexing
    call PB.MakeSummarizedDemultiplexingReport as SummarizedDemuxReportPNG { input: report = Demultiplex.report }
    call PB.MakeDetailedDemultiplexingReport as DetailedDemuxReportPNG { input: report = Demultiplex.report, type="png" }

    scatter (demux_bam in Demultiplex.demux_bams) {
        String BC = sub(basename(demux_bam, ".bam"), "~{participant_name}.corrected", "")

        # tag BAM
        call PB.Tag { input: bam = demux_bam }

        call PB.RefineTranscriptReads {
            input:
                bam          = Tag.tagged_bam,
                barcode_file = barcode_file,
                prefix       = "~{participant_name}.~{BC}.flnc"
        }

        call PB.Align as AlignRefinedReads {
            input:
                bam          = RefineTranscriptReads.refined_bam,
                ref_fasta    = ref_map['fasta'],
                sample_name  = participant_name,
                drop_per_base_N_pulse_tags = drop_per_base_N_pulse_tags,
                map_preset   = "ISOSEQ",
                prefix       = "~{participant_name}.~{BC}",
                runtime_attr_override = { "cpu_cores": 32 }
        }

        call PB.Dedup { input: bam = RefineTranscriptReads.refined_bam }

        call PB.Align as AlignTranscripts {
            input:
                bam          = Dedup.deduped_bam,
                ref_fasta    = ref_map['fasta'],
                sample_name  = participant_name,
                drop_per_base_N_pulse_tags = drop_per_base_N_pulse_tags,
                map_preset   = "ISOSEQ",
                prefix       = "~{participant_name}.~{BC}",
                runtime_attr_override = { "cpu_cores": 32 }
        }

        # create a BED file that indicates where the BAM file has coverage
        call Utils.BamToBed { input: bam = AlignTranscripts.aligned_bam, prefix = BC }

        call SQANTI.QC {
            input:
                bam = AlignTranscripts.aligned_bam,
                ref_fasta = ref_map['fasta'],
                ref_gtf = ref_gtf,
                prefix = BC
        }

        ##########
        # store the demultiplexing results into designated bucket
        ##########

        String adir = outdir + "/alignments/" + BC
        String tdir = outdir + "/transcripts/" + BC
        String qdir = outdir + "/qc/" + BC

        call FF.FinalizeToFile as FinalizeAlignedTranscriptsBam { input: outdir = adir, file = AlignTranscripts.aligned_bam }
        call FF.FinalizeToFile as FinalizeAlignedTranscriptsBai { input: outdir = adir, file = AlignTranscripts.aligned_bai }
        call FF.FinalizeToFile as FinalizeAlignedTranscriptsBed { input: outdir = adir, file = BamToBed.bed }

        call FF.FinalizeToFile as FinalizeClassifications { input: outdir = qdir, file = QC.classification }
        call FF.FinalizeToFile as FinalizeJunctions { input: outdir = qdir, file = QC.junctions }
        call FF.FinalizeToFile as FinalizeReport { input: outdir = qdir, file = QC.report }
    }

    # merge demultiplexed BAMs into a single BAM (one readgroup per file)
    call Utils.MergeBams as MergeBarcodeBams { input: bams = AlignTranscripts.aligned_bam, prefix = "barcodes" }

    call PB.PBIndex as IndexAlignedReads { input: bam = MergeBarcodeBams.merged_bam }

    # Finalize
    String rdir = outdir + "/reads"
    String bdir = outdir + "/alignments/all_barcodes"
    String mdir = outdir + "/metrics/combined/lima"
    String fdir = outdir + "/figures"

    call FF.FinalizeToFile as FinalizeBam { input: outdir = rdir, file = bam, name = "~{participant_name}.bam" }
    call FF.FinalizeToFile as FinalizePbi { input: outdir = rdir, file = pbi, name = "~{participant_name}.bam.pbi" }

    call FF.FinalizeToFile as FinalizeAlignedBam { input: outdir = bdir, file = MergeBarcodeBams.merged_bam, name = "~{participant_name}.all_barcodes.bam" }
    call FF.FinalizeToFile as FinalizeAlignedBai { input: outdir = bdir, file = MergeBarcodeBams.merged_bai, name = "~{participant_name}.all_barcodes.bam.bai"  }
    call FF.FinalizeToFile as FinalizeAlignedPbi { input: outdir = bdir, file = IndexAlignedReads.pbi, name = "~{participant_name}.all_barcodes.bam.pbi"  }

    call FF.FinalizeToFile as FinalizeDemultiplexCounts { input: outdir = mdir, file = Demultiplex.counts, name = "~{participant_name}.lima.counts.txt" }
    call FF.FinalizeToFile as FinalizeDemultiplexReport { input: outdir = mdir, file = Demultiplex.report, name = "~{participant_name}.lima.report.txt" }
    call FF.FinalizeToFile as FinalizeDemultiplexSummary { input: outdir = mdir, file = Demultiplex.summary, name = "~{participant_name}.lima.summary.txt" }

    call FF.FinalizeToDir as FinalizeLimaSummary { input: outdir = fdir + "/summary/png", files = SummarizedDemuxReportPNG.report_files }
    call FF.FinalizeToDir as FinalizeLimaDetailedPNG { input: outdir = fdir + "/detailed/png", files = DetailedDemuxReportPNG.report_files }

    output {
        File ccs_bam = FinalizeBam.gcs_path
        File ccs_pbi = FinalizePbi.gcs_path

        File aligned_bam = FinalizeAlignedBam.gcs_path
        File aligned_bai = FinalizeAlignedBai.gcs_path
        File aligned_pbi = FinalizeAlignedPbi.gcs_path

        File demux_counts = FinalizeDemultiplexCounts.gcs_path
        File demux_reports = FinalizeDemultiplexReport.gcs_path
        File demux_summary = FinalizeDemultiplexSummary.gcs_path
    }
}
