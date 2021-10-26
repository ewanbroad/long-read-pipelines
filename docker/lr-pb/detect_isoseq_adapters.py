import argparse
import pysam


def main():
    parser = argparse.ArgumentParser(description='Detect IsoSeq adapters', prog='detect_isoseq_adapters')
    parser.add_argument('bam', type=str, help="BAM file")
    args = parser.parse_args()

    # Silence message about the .bai file not being found.
    pysam.set_verbosity(0)
    with pysam.AlignmentFile(args.bam, 'rb', check_sq=False, check_header=False) as ibam:
        for r in ibam:
            #print(r)

            if r.cigartuples is not None:
                if r.cigartuples[0][0] == 4: # left soft-clip:
                    soft_clip_left = r.query_sequence[0:r.cigartuples[0][1]]
                    print(f'>{r.query_name}_left')
                    print(soft_clip_left)

                if r.cigartuples[-1][0] == 4: # right soft-clip:
                    soft_clip_right = r.query_sequence[(len(r.query_sequence) - r.cigartuples[-1][1]):]
                    print(f'>{r.query_name}_right')
                    print(soft_clip_right)


if __name__ == "__main__":
    main()
