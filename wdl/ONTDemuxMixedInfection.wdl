version 1.0

######################################################################################
## A workflow that demultiplexes ONT data representing mixed infections.
######################################################################################

import "tasks/Utils.wdl" as Utils
import "tasks/CallVariantsONT.wdl" as VAR
import "tasks/Finalize.wdl" as FF

workflow ONTDemuxMixedInfection {
    input {
        File aligned_bam
        File aligned_bai
        File ref_map_file

        String dir_prefix
        String gcs_out_root_dir
    }

    parameter_meta {
        bam:                "GCS path to aligned BAM file"
        bai:                "GCS path to aligned BAM file index"
        ref_map_file:       "table indicating reference sequence and auxillary file locations"

        gcs_out_root_dir:   "GCS bucket to store the reads, variants, and metrics files"
    }

    Map[String, String] ref_map = read_map(ref_map_file)

    String outdir = sub(gcs_out_root_dir, "/$", "") + "/ONTDemuxMixedInfection/~{dir_prefix}"

    call Utils.ComputeGenomeLength { input: fasta = ref_map['fasta'] }

    call VAR.CallVariants {
        input:
            bam               = aligned_bam,
            bai               = aligned_bai,

            ref_fasta         = ref_map['fasta'],
            ref_fasta_fai     = ref_map['fai'],
            ref_dict          = ref_map['dict'],
            tandem_repeat_bed = ref_map['tandem_repeat_bed'],

            prefix            = dir_prefix
    }

    #call CreateVariantGraph {}
    #call AlignReadsToGraph {}

    # Finalize data
#    String dir = outdir + "/assembly"

#    call FF.FinalizeToFile as FinalizeHifiasmGfa { input: outdir = dir, file = Hifiasm.gfa }
#    call FF.FinalizeToFile as FinalizeHifiasmFa { input: outdir = dir, file = Hifiasm.fa }
#    call FF.FinalizeToFile as FinalizeQuastReportHtml { input: outdir = dir, file = Quast.report_html }
#    call FF.FinalizeToFile as FinalizeQuastReportTxt { input: outdir = dir, file = Quast.report_txt }
#    call FF.FinalizeToFile as FinalizePaf { input: outdir = dir, file = CallAssemblyVariants.paf }
#    call FF.FinalizeToFile as FinalizePafToolsVcf { input: outdir = dir, file = CallAssemblyVariants.paftools_vcf }

    output {
    }
}