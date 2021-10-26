version 1.0

import "tasks/Structs.wdl"
import "tasks/Finalize.wdl" as FF

workflow VerifyFingerprint {

    meta {
        description: "A workflow to detect potential sample swaps using Picard fingerprint verification tools"
    }

    input {
        File aligned_bam
        File aligned_bai

        File fingerprint_vcf

        File ref_map_file

        String gcs_out_root_dir
    }

    parameter_meta {
        aligned_bam:        "GCS path to aligned BAM file, supposed to be of the same sample as from the fingerprinting VCF"

        fingerprint_vcf:    "Fingerprint VCF file from local database; note that sample name must be the same as in BAM"

        ref_map_file:       "table indicating reference sequence and auxillary file locations"

        gcs_out_root_dir:   "GCS bucket to store the reads, variants, and metrics files"
    }

    Map[String, String] ref_map = read_map(ref_map_file)

    String outdir = sub(gcs_out_root_dir, "/$", "") + "/VerifyFingerprint"

    call CheckFingerprint {
        input:
            aligned_bam     = aligned_bam,
            aligned_bai     = aligned_bai,
            fingerprint_vcf = fingerprint_vcf,
            haplotype_map   = ref_map['haplotype_map']
    }

    call FF.FinalizeToFile as FinalizeFingerprintSummaryMetrics { input: outdir = outdir, file = CheckFingerprint.summary_metrics }
    call FF.FinalizeToFile as FinalizeFingerprintDetailMetrics { input: outdir = outdir, file = CheckFingerprint.detail_metrics }

    output {
        Float lod_expected_sample = CheckFingerprint.metrics_map['LOD_EXPECTED_SAMPLE']

        File fingerprint_metrics = FinalizeFingerprintSummaryMetrics.gcs_path
        File fingerprint_details = FinalizeFingerprintDetailMetrics.gcs_path
    }
}

task CheckFingerprint {

    meta {
        description: "Uses Picard tool CheckFingerprint to verify if the samples in provided VCF and BAM arise from the same biological sample"
    }
    input {
        File aligned_bam
        File aligned_bai

        File fingerprint_vcf
        Array[String] filters = ['random', 'chrUn', 'decoy', 'alt', 'HLA', 'EBV']

        File haplotype_map

        RuntimeAttr? runtime_attr_override
    }

    parameter_meta {
        aligned_bam:{
            description:  "GCS path to aligned BAM file, supposed to be of the same sample as from the fingerprinting VCF",
            localization_optional: true
        }

        fingerprint_vcf:    "Fingerprint VCF file from local database; note that sample name must be the same as in BAM"

        filters:            "An array of chromosome names to filter out when verifying fingerprints"

        haplotype_map:      "table indicating reference sequence and auxillary file locations"
    }

    Int disk_size = ceil(size([fingerprint_vcf, haplotype_map], "GB"))
    String prefix = basename(aligned_bam, ".bam")

    command <<<
        set -x

        grep \
            -v \
            -e ' placeholder ' \
            ~{true='-e' false='' length(filters) > 0} \
            ~{sep=" -e " filters} \
            ~{fingerprint_vcf}  \
            > fingerprint.fixed.vcf

        gatk CheckFingerprint \
            --INPUT ~{aligned_bam} \
            --GENOTYPES fingerprint.fixed.vcf \
            --HAPLOTYPE_MAP ~{haplotype_map} \
            --OUTPUT ~{prefix}

        grep -v '^#' ~{prefix}.fingerprinting_summary_metrics | \
            grep -A1 READ_GROUP | \
            awk '
                {
                    for (i=1; i<=NF; i++)  {
                        a[NR,i] = $i
                    }
                }
                NF>p { p = NF }
                END {
                    for(j=1; j<=p; j++) {
                        str=a[1,j]
                        for(i=2; i<=NR; i++){
                            str=str" "a[i,j];
                        }
                        print str
                    }
                }' | \
            sed 's/ /\t/' \
            > metrics_map.txt

        mv ~{prefix}.fingerprinting_summary_metrics \
            ~{prefix}.fingerprinting_summary_metrics.txt
        mv ~{prefix}.fingerprinting_detail_metrics \
            ~{prefix}.fingerprinting_detail_metrics.txt
    >>>

    output {
        File summary_metrics = "~{prefix}.fingerprinting_summary_metrics.txt"
        File detail_metrics = "~{prefix}.fingerprinting_detail_metrics.txt"
        Map[String, String] metrics_map = read_map("metrics_map.txt")
    }

    ###################
    RuntimeAttr default_attr = object {
        cpu_cores:             2,
        mem_gb:                4,
        disk_gb:               disk_size,
        boot_disk_gb:          10,
        preemptible_tries:     3,
        max_retries:           2,
        docker:                "us.gcr.io/broad-gatk/gatk:4.2.0.0"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                   select_first([runtime_attr.cpu_cores, default_attr.cpu_cores])
        memory:                select_first([runtime_attr.mem_gb, default_attr.mem_gb]) + " GiB"
        disks: "local-disk " + select_first([runtime_attr.disk_gb, default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:        select_first([runtime_attr.boot_disk_gb, default_attr.boot_disk_gb])
        preemptible:           select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:            select_first([runtime_attr.max_retries, default_attr.max_retries])
        docker:                select_first([runtime_attr.docker, default_attr.docker])
    }
}