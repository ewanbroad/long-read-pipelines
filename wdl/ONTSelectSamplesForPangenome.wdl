version 1.0

import "tasks/ONTUtils.wdl" as ONT
import "tasks/Utils.wdl" as Utils
import "tasks/McCortex.wdl" as McCortex
import "tasks/Finalize.wdl" as FF

workflow ONTSelectSamplesForPangenome {
    input {
        String input_dir
        File ref_map_file

        String gcs_out_root_dir
    }

    String outdir = sub(gcs_out_root_dir, "/$", "") + "/ONTSelectSamplesForPangenome/"

    call Utils.ListFilesOfType {
         input:
            gcs_dir = input_dir,
            suffixes = [ ".bam" ]
    }

    scatter (bam in ListFilesOfType.files) {
        call McCortex.Assemble { input: reads = bam, sample = basename(bam, ".bam") }
    }

    output {
    }
}