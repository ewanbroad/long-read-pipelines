FROM continuumio/miniconda3

MAINTAINER Kiran V Garimella

# copy other resources
COPY ./environment.yml /

# install conda packages
RUN conda env create -f /environment.yml && conda clean -a
ENV PATH=/opt/conda/envs/lr-asm/bin/:/root/google-cloud-sdk/bin/:${PATH}

# install minimap2
RUN wget https://github.com/lh3/minimap2/releases/download/v2.23/minimap2-2.23_x64-linux.tar.bz2 \
        && tar xjf minimap2-2.23_x64-linux.tar.bz2 \
        && rm minimap2-2.23_x64-linux.tar.bz2 \
        && cd minimap2-2.23_x64-linux \
        && cp minimap2 /usr/local/bin/ \
        && cp k8 /usr/local/bin/ \
        && cp paftools.js /usr/local/bin/

# install Shasta
RUN wget https://github.com/chanzuckerberg/shasta/releases/download/0.8.0/shasta-Linux-0.8.0 \
        && chmod ugo+x shasta-Linux-0.8.0 \
        && mv shasta-Linux-0.8.0 /usr/local/bin/shasta

# install Canu
RUN wget https://github.com/marbl/canu/releases/download/v2.2/canu-2.2.Linux-amd64.tar.xz \
        && tar xf canu-2.2.Linux-amd64.tar.xz \
        && cp canu-2.2/bin/* /usr/local/bin/ \
        && cp -R canu-2.2/lib/site_perl /usr/local/lib/ \
        && cp -R canu-2.2/share/java /usr/local/share/

# copy additional scripts
COPY ./dedup_reads_from_same_zmw.py /usr/local/bin/
COPY ./cat_as_fasta.py /usr/local/bin/

# activate conda environment
RUN echo "source activate lr-asm" >> ~/.bashrc

# set LD library path
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/conda/envs/lr-asm/lib/
