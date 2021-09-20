version 1.0

import "Structs.wdl"
import "Utils.wdl" as Utils

workflow C3POa {
    input {
        File manifest_chunk
        File ref_fasta
        File splint_fasta
    }

    call Cat as CatRawReads { input: files = read_lines(manifest_chunk), out = "chunk.fastq" }

    call Processing { input: fastq = CatRawReads.merged, splint_fasta = splint_fasta }

    call Postprocessing as Postprocessing1 { input: consensus = Processing.consensus1 }
    call Postprocessing as Postprocessing2 { input: consensus = Processing.consensus2 }
    call Postprocessing as Postprocessing3 { input: consensus = Processing.consensus3 }
    call Postprocessing as Postprocessing4 { input: consensus = Processing.consensus4 }

    output {
        File subreads1 = Processing.subreads1
        File subreads2 = Processing.subreads2
        File subreads3 = Processing.subreads3
        File subreads4 = Processing.subreads4

        File consensus1 = Postprocessing1.consensus_full
        File consensus2 = Postprocessing2.consensus_full
        File consensus3 = Postprocessing3.consensus_full
        File consensus4 = Postprocessing4.consensus_full

        Int no_splint_reads  = Processing.no_splint_reads
        Int under_len_cutoff = Processing.under_len_cutoff
        Int total_reads      = Processing.total_reads
    }
}

task Processing {
    input {
        File fastq
        File splint_fasta

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 4*ceil(size(fastq, "GB"))

    command <<<
        set -euxo pipefail

        num_core=$(cat /proc/cpuinfo | awk '/^processor/{print $3}' | wc -l)

        mkdir out
        python3 /C3POa/C3POa.py \
            -r ~{fastq} \
            -s ~{splint_fasta} \
            -c /c3poa.config.txt \
            -l 100 -d 500 -n $num_core -g 1000 \
            -o out

        grep 'No splint reads' out/c3poa.log | awk '{ print $4 }' > no_splint_reads.txt
        grep 'Under len cutoff' out/c3poa.log | awk '{ print $4 }' > under_len_cutoff.txt
        grep 'Total reads' out/c3poa.log | awk '{ print $3 }' > total_reads.txt
        grep 'Reads after preprocessing' out/c3poa.log | awk '{ print $4 }' > reads_after_preprocessing.txt
        grep -c '>' out/10x_Splint_*/R2C2_Consensus.fasta > reads_after_consensus.txt

        tree -h
    >>>

    output {
        File consensus1 = "out/10x_Splint_1/R2C2_Consensus.fasta"
        File consensus2 = "out/10x_Splint_2/R2C2_Consensus.fasta"
        File consensus3 = "out/10x_Splint_3/R2C2_Consensus.fasta"
        File consensus4 = "out/10x_Splint_4/R2C2_Consensus.fasta"

        File subreads1 = "out/10x_Splint_1/R2C2_Subreads.fastq"
        File subreads2 = "out/10x_Splint_2/R2C2_Subreads.fastq"
        File subreads3 = "out/10x_Splint_3/R2C2_Subreads.fastq"
        File subreads4 = "out/10x_Splint_4/R2C2_Subreads.fastq"

        File c3poa_log = "out/c3poa.log"
        Int no_splint_reads = read_int("no_splint_reads.txt")
        Int under_len_cutoff = read_int("under_len_cutoff.txt")
        Int total_reads = read_int("total_reads.txt")
        Int reads_after_preprocessing = read_int("reads_after_preprocessing.txt")
        Int reads_after_consensus = read_int("reads_after_consensus.txt")
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          4,
        mem_gb:             8,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  1,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-c3poa:2.2.3"
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

task Postprocessing {
    input {
        File consensus

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 2*ceil(size(consensus, "GB"))

    command <<<
        set -euxo pipefail

        python3 /C3POa/C3POa_postprocessing.py \
            -i ~{consensus} \
            -c /c3poa.config.txt \
            -a /C3POa/adapter.fasta \
            -o ./

        tree -h
    >>>

    output {
        File consensus_full = "R2C2_full_length_consensus_reads.fasta"
        File consensus_left = "R2C2_full_length_consensus_reads_left_splint.fasta"
        File consensus_right = "R2C2_full_length_consensus_reads_right_splint.fasta"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             2,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  2,
        max_retries:        1,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-c3poa:2.2.3"
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

task Cat {
    input {
        Array[File] files
        String out

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 1 + 3*ceil(size(files, "GB"))

    command <<<
        set -euxo pipefail

        cat ~{sep=' ' files} > ~{out}
    >>>

    output {
        File merged = "~{out}"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             2,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  2,
        max_retries:        1,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-c3poa:2.2.3"
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

task Annotate {
    input {
        File bam

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 3*ceil(size(bam, "GB"))

    command <<<
        set -euxo pipefail

        samtools fasta ~{bam} > R2C2_10x_postprocessed.fasta

        python3 /10xR2C2/demuxing/detBarcodes.py R2C2_10x_postprocessed.fasta /737K-august-2016.txt > 1500_most_frequent_bcs.fasta
        python3 /10xR2C2/demuxing/Demultiplex_R2C2_reads_kmerBased.py -i R2C2_10x_postprocessed.fasta -o . -n 1500_most_frequent_bcs.fasta
        python3 /10xR2C2/demuxing/match_fastas.py kmer_demuxed.fasta R2C2_10x_postprocessed.fasta > R2C2_matched.fasta

        mkdir -p demuxed

        python3 /10xR2C2/demuxing/demux_nano.py 1500_most_frequent_bcs.fasta kmer_demuxed.fasta R2C2_matched.fasta

        tree -h
    >>>

    output {
        File most_frequent_1500_bcs_fa = "1500_most_frequent_bcs.fasta"
        File R2C2_10x_postprocessed_fa = "R2C2_10x_postprocessed.fasta"
        File R2C2_matched_fa = "R2C2_matched.fasta"
        File bcGuide = "demuxed/bcGuide"
        Array[File] cell_barcoded_fa = glob("demuxed/cell*.fasta") # e.g. cell_0_GTGCGGTTCCTGTAGA.fasta
        File kmer_demuxed_fa = "kmer_demuxed.fasta"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             8,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  2,
        max_retries:        1,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-c3poa:2.2.3"
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
