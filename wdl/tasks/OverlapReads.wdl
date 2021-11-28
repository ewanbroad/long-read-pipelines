version 1.0

import "Structs.wdl"

task Minimap2 {
    input {
        File reads

        String map_preset
        Int max_gap = 5000
        Int num_threads = 16

        String prefix = "out"
        RuntimeAttr? runtime_attr_override
    }

    parameter_meta {
        reads:       "query sequences to be mapped to each another"
        map_preset:  "preset to be used for minimap2 parameter '-x'"
        max_gap:     "stop chain enlongation if there are no minimizers in INT-bp"
        num_threads: "number of threads to use"
        prefix:      "[default-valued] prefix for output PAF"
    }

    Int disk_size = 1 + 20*ceil(size(reads, "GB"))
    Int mem = 96

    command <<<
        set -euxo pipefail

        minimap2 -x ~{map_preset} -t ~{num_threads} -g ~{max_gap} ~{reads} ~{reads} | gzip > ~{prefix}.paf.gz
    >>>

    output {
        File paf_gz = "~{prefix}.paf.gz"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          num_threads,
        mem_gb:             mem,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  1,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-asm:0.1.14"
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