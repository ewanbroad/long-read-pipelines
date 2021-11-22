version 1.0

import "Structs.wdl"

task QC {
    input {
        File bam
        File ref_fasta
        File ref_gtf

        String prefix = "out"

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 20*ceil(size(bam, "GB")) + 1

    command <<<
        set -euxo pipefail

        samtools view -h ~{bam} > ~{prefix}.sam
        python3 /cDNA_Cupcake/sequence/sam_to_collapsed_gff.py ~{prefix}.sam
        python3 /SQANTI3/sqanti3_qc.py --report pdf -t 4 ~{prefix}.collapsed.gff ~{ref_gtf} ~{ref_fasta}
    >>>

    output {
        File classification = "~{prefix}.collapsed_classification.txt"
        File junctions = "~{prefix}.collapsed_junctions.txt"
        File report = "~{prefix}.collapsed_SQANTI3_report.pdf"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          2,
        mem_gb:             8,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  0,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-sqanti:4.2"
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
