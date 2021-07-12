version 1.0

import "Structs.wdl"
import "Finalize.wdl" as FF

workflow VariantMetrics {
    input {
        File snv_vcf
        File sv_vcf

        File ref_fasta
        File ref_dict
        File ref_fai
    }

    call ReadMetrics as AlignedReadMetrics { input: bam = aligned_bam }

    output {
        File aligned_flag_stats = AlignedFlagStats.flag_stats
    }
}

task VCFStats {
    input {
        File vcf
        String? include
        String? exclude

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 2*ceil(size(vcf, "GB")) + 1

    command <<<
        set -euxo pipefail

        bcftools \
            ~{true='-i' false='' defined(include)} ~{select_first([include, ""])} \
            ~{true='-e' false='' defined(exclude)} ~{select_first([exclude, ""])} \
            bcftools stats > stats.txt
    >>>

    output {
        Array[Array[String]] chrs = read_tsv("chrs.txt")
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             1,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  2,
        max_retries:        1,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-sv:0.1.8"
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