version 1.0

######################################################################################
## A workflow that demultiplexes ONT data representing mixed infections.
######################################################################################

import "tasks/Utils.wdl" as Utils
import "tasks/OverlapReads.wdl" as OVLP
import "tasks/Finalize.wdl" as FF

workflow ONTDemuxMixedInfection {
    input {
        String fastq_dir
        File ref_map_file

        String dir_prefix
        String gcs_out_root_dir
    }

    parameter_meta {
        fastq_dir:          "GCS path to fastq directory"
        ref_map_file:       "table indicating reference sequence and auxillary file locations"

        gcs_out_root_dir:   "GCS bucket to store the reads, variants, and metrics files"
    }

    Map[String, String] ref_map = read_map(ref_map_file)

    String outdir = sub(gcs_out_root_dir, "/$", "") + "/ONTDemuxMixedInfection/~{dir_prefix}"

    call Utils.ComputeGenomeLength { input: fasta = ref_map['fasta'] }

    call Utils.ListFilesOfType {
        input:
            gcs_dir = fastq_dir,
            suffixes = [ '.fastq', '.fastq.gz', '.fq', '.fq.gz' ],
            recurse = true
    }

    call Utils.Cat as CombineFastqs { input: files = ListFilesOfType.files, out = "all.fq" }

    call OVLP.Minimap2 {
        input:
            reads = CombineFastqs.combined,
            map_preset = 'ava-ont',
            max_gap = 500
    }

#    call CreateVariantGraph {}
#    call AlignReadsToGraph {}

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
