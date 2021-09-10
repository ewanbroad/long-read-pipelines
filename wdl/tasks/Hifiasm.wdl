version 1.0

import "Structs.wdl"

workflow Hifiasm {
    input {
        File reads
        String prefix
    }

    parameter_meta {
        reads:    "reads (in fasta or fastq format, compressed or uncompressed)"
        prefix:   "prefix to apply to assembly output filenames"
    }

    call Assemble {
        input:
            reads  = reads,
            prefix = prefix
    }

    output {
        File gfa = Assemble.gfa
        File fa = Assemble.fa
        Array[File] phased_contigs = Assemble.phased_contigs
    }
}

task Assemble {
    input {
        File reads
        String prefix = "out"

        RuntimeAttr? runtime_attr_override
    }
    
    Int memory = 3 * ceil(size(reads, "GB"))
    Int n = memory / 4  # this might be an odd number
    Int num_cpus = if (n/2)*2 == n then n else n+1  # a hack because WDL doesn't have modulus operator

    Int disk_size = 10 * ceil(size(reads, "GB"))

    command <<<
        set -euxo pipefail

        hifiasm \
            -o ~{prefix} \
            -t~{num_cpus} \
            ~{reads}
        
        awk '/^S/{print ">"$2; print $3}' \
            ~{prefix}.bp.p_ctg.gfa \
            > ~{prefix}.bp.p_ctg.fa
    >>>

    output {
        File gfa = "~{prefix}.bp.p_ctg.gfa"
        File fa = "~{prefix}.bp.p_ctg.fa"
        Array[File] phased_contigs = glob("~{prefix}.bp.hap*.p_ctg.gfa")
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          num_cpus,
        mem_gb:             memory,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  0,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-hifiasm:0.16.0"
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
