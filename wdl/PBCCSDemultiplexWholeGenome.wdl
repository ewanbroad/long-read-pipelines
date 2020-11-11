version 1.0

##########################################################################################
## A workflow that performs CCS correction, demultiplexing, and variant calling on PacBio
## HiFi reads from a single flow cell. The workflow shards the subreads into clusters and
## performs CCS in parallel on each cluster.  Error-corrected reads are then demultiplexed,
## and each demultiplexed dataset is independently variant-called.  A number of metrics
## and figures are produced along the way.
##########################################################################################

import "tasks/PBUtils.wdl" as PB
import "tasks/Utils.wdl" as Utils
import "tasks/AlignReads.wdl" as AR
import "tasks/AlignedMetrics.wdl" as AM
import "tasks/Finalize.wdl" as FF
import "tasks/CallSVs.wdl" as SV
import "tasks/CallSmallVariants.wdl" as SMV

workflow PBCCSDemultiplexWholeGenome {
    input {
        Array[File] bams
        File ref_map_file

        String participant_name
        File barcode_file
        Int num_shards = 300

        String? gcs_out_root_dir
    }

    parameter_meta {
        bams:             "GCS path to raw subreads or CCS data"
        ref_map_file:     "table indicating reference sequence and auxillary file locations"

        participant_name: "name of the participant from whom these samples were obtained"
        barcode_file:     "GCS path to the fasta file that specifies the expected set of multiplexing barcodes"
        num_shards:       "[default-valued] number of sharded BAMs to create (tune for performance)"

        gcs_out_root_dir: "[optional] GCS bucket to store the corrected/uncorrected reads, variants, and metrics files"
    }

    Map[String, String] ref_map = read_map(ref_map_file)

    call Utils.GetDefaultDir { input: workflow_name = "PBCCSDemultiplexWholeGenome" }
    String outdir = sub(select_first([gcs_out_root_dir, GetDefaultDir.path]), "/$", "") + "/" + participant_name

    # scatter over all sample BAMs
    scatter (bam in bams) {
        File pbi = sub(bam, ".bam$", ".bam.pbi")

        call PB.GetRunInfo { input: bam = bam }
        String ID = GetRunInfo.run_info["PU"]

        # break one raw BAM into fixed number of shards
        call PB.ShardLongReads { input: unaligned_bam = bam, unaligned_pbi = pbi, num_shards = num_shards }

        # then perform correction on each of the shard
        scatter (subreads in ShardLongReads.unmapped_shards) {
            call PB.CCS { input: subreads = subreads }
        }

        # merge the corrected per-shard BAM/report into one, corresponding to one raw input BAM
        call Utils.MergeBams as MergeCorrected { input: bams = CCS.consensus, prefix = "~{participant_name}.~{ID}.corrected" }
        call PB.MergeCCSReports as MergeCCSReports { input: reports = CCS.report }
    }

    # gather across (potential multiple) input raw BAMs
    if (length(bams) > 1) {
        call Utils.MergeBams as MergeAllCorrected { input: bams = MergeCorrected.merged_bam, prefix = "~{participant_name}.corrected" }
        call PB.MergeCCSReports as MergeAllCCSReports { input: reports = MergeCCSReports.report }
    }

    File ccs_bam = select_first([ MergeAllCorrected.merged_bam, MergeCorrected.merged_bam[0] ])
    File ccs_bai = select_first([ MergeAllCorrected.merged_bai, MergeCorrected.merged_bai[0] ])
    File ccs_report = select_first([ MergeAllCCSReports.report, MergeCCSReports.report[0] ])

    # demultiplex CCS-ed BAM
    call PB.Demultiplex {
        input:
            bam = ccs_bam,
            prefix = participant_name,
            barcode_file = barcode_file,
            ccs = true,
            guess = 75,
            guess_min_count = 1,
            dump_removed = true,
            split_bam_named = true
    }

    # make reports on demultiplexing
    call PB.MakeSummarizedDemultiplexingReport as SummarizedDemuxReportPNG { input: report = Demultiplex.report }
    call PB.MakeDetailedDemultiplexingReport as DetailedDemuxReportPNG { input: report = Demultiplex.report, type="png" }
    call PB.MakeDetailedDemultiplexingReport as DetailedDemuxReportPDF { input: report = Demultiplex.report, type="pdf" }
    call PB.MakePerBarcodeDemultiplexingReports as PerBarcodeDetailedDemuxReportPNG { input: report = Demultiplex.report, type="png" }
    call PB.MakePerBarcodeDemultiplexingReports as PerBarcodeDetailedDemuxReportPDF { input: report = Demultiplex.report, type="pdf" }

    # scatter on each demultiplexed BAM file
    scatter (demux_bam in Demultiplex.demux_bams) {
        String BC = sub(basename(demux_bam, ".bam"), "~{participant_name}.corrected", "")

        # align reads to reference
        call PB.Align as AlignBarcode {
            input:
                bam         = demux_bam,
                ref_fasta   = ref_map['fasta'],
                sample_name = participant_name,
                map_preset  = "CCS"
        }

        # compute alignment metrics
        call AM.AlignedMetrics as PerBarcodeMetrics {
            input:
                aligned_bam    = AlignBarcode.aligned_bam,
                aligned_bai    = AlignBarcode.aligned_bai,
                ref_fasta      = ref_map['fasta'],
                ref_dict       = ref_map['dict'],
                ref_flat       = ref_map['flat'],
                dbsnp_vcf      = ref_map['dbsnp_vcf'],
                dbsnp_tbi      = ref_map['dbsnp_tbi'],
                metrics_locus  = ref_map['metrics_locus'],
                gcs_output_dir = outdir + "/" + BC + "/metrics/per_barcode"
        }

        # create a BED file that indicates where the BAM file has coverage
        call Utils.BamToBed { input: bam = AlignBarcode.aligned_bam, prefix = BC }

        # call SVs
        call SV.CallSVs as CallSVs {
            input:
                bam               = ccs_bam,
                bai               = ccs_bai,

                ref_fasta         = ref_map['fasta'],
                ref_fasta_fai     = ref_map['fai'],
                tandem_repeat_bed = ref_map['tandem_repeat_bed'],

                preset            = "hifi"
        }

        # call SNVs and small indels
        call SMV.CallSmallVariants as CallSmallVariants {
            input:
                bam               = ccs_bam,
                bai               = ccs_bai,

                ref_fasta         = ref_map['fasta'],
                ref_fasta_fai     = ref_map['fai'],
                ref_dict          = ref_map['dict'],
        }

        ##########
        # store the demultiplexing results into designated bucket
        ##########

        call FF.FinalizeToDir as FinalizeDemuxAlignedReads {
            input:
                files  = [ AlignBarcode.aligned_bam, AlignBarcode.aligned_bai, BamToBed.bed ],
                outdir = outdir + "/" + BC + "/alignments"
        }

        call FF.FinalizeToDir as FinalizeSVs {
            input:
                files = [ CallSVs.pbsv_vcf, CallSVs.sniffles_vcf, CallSVs.svim_vcf, CallSVs.cutesv_vcf ],
                outdir = outdir + "/" + BC + "/variants"
        }

        call FF.FinalizeToDir as FinalizeSmallVariants {
            input:
                files = [ CallSmallVariants.longshot_vcf, CallSmallVariants.longshot_tbi ],
                outdir = outdir + "/" + BC + "/variants"
        }
    }

    # merge demultiplexed BAMs into a single BAM (one readgroup per file)
    call Utils.MergeBams as MergeBarcodeBams { input: bams = AlignBarcode.aligned_bam, prefix = "barcodes" }

    ##########
    # store the results into designated bucket
    ##########

    call FF.FinalizeToDir as FinalizeDemuxCombinedReads {
        input:
            files = [ MergeBarcodeBams.merged_bam, MergeBarcodeBams.merged_bai ],
            outdir = outdir + "/combined/alignments"
    }

    call FF.FinalizeToDir as FinalizeCCSMetrics {
        input:
            files = [ ccs_report ],
            outdir = outdir + "/metrics/combined/" + participant_name + "/ccs_metrics"
    }

    call FF.FinalizeToDir as FinalizeLimaMetrics {
        input:
            files = [ Demultiplex.counts, Demultiplex.report, Demultiplex.summary ],
            outdir = outdir + "/metrics/combined/" + participant_name + "/lima"
    }

    call FF.FinalizeToDir as FinalizeLimaSummary {
        input:
            files = SummarizedDemuxReportPNG.report_files,
            outdir = outdir + "/figures/combined/" + participant_name + "/lima/summary/png"
    }

    call FF.FinalizeToDir as FinalizeLimaDetailedPNG {
        input:
            files = DetailedDemuxReportPNG.report_files,
            outdir = outdir + "/figures/combined/" + participant_name + "/lima/detailed/png"
    }

    call FF.FinalizeToDir as FinalizeLimaDetailedPDF {
        input:
            files = DetailedDemuxReportPDF.report_files,
            outdir = outdir + "/figures/combined/" + participant_name + "/lima/detailed/pdf"
    }

    call FF.FinalizeToDir as FinalizeLimaPerBarcodeDetailedPNG {
        input:
            files = PerBarcodeDetailedDemuxReportPNG.report_files,
            outdir = outdir + "/figures/per_barcode/lima/png"
    }

    call FF.FinalizeToDir as FinalizeLimaPerBarcodeDetailedPDF {
        input:
            files = PerBarcodeDetailedDemuxReportPDF.report_files,
            outdir = outdir + "/figures/per_barcode/lima/pdf"
    }
}
