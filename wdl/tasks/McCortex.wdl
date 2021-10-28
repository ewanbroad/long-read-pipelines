version 1.0

import "Structs.wdl"

task Assemble {
    input {
        File reads
        String sample
        Int k = 31

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 3*(ceil(size(reads, "GB")))
    Int mem = 4*(ceil(size(reads, "GB")))

    command <<<
        set -euxo pipefail

        FILE="~{reads}"
        if [[ "$FILE" =~ \.fasta$ ]] || [[ "$FILE" =~ \.fa$ ]]; then
            SEQ_TYPE="--seq"
        else
            SEQ_TYPE="--seqi"
        fi

        mccortex ~{k} build -m ~{mem}G -S -k ~{k} -s ~{sample} $SEQ_TYPE ~{reads} ~{sample}.k~{k}.ctx
        mccortex ~{k} clean -m ~{mem}G -S -B 1 -o ~{sample}.k~{k}.clean.ctx ~{sample}.k~{k}.ctx
    >>>

    output {
        File ctx = "~{sample}.k~{k}.clean.ctx"
    }

    ###################
    RuntimeAttr default_attr = object {
        cpu_cores:             1,
        mem_gb:                mem + 2,
        disk_gb:               disk_size,
        boot_disk_gb:          10,
        preemptible_tries:     1,
        max_retries:           0,
        docker:                "us.gcr.io/broad-dsp-lrma/lr-mccortex:1.0.0"
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
