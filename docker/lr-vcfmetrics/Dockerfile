FROM continuumio/miniconda3

MAINTAINER Kiran V Garimella

# copy other resources
COPY ./environment.yml /

# install conda packages
RUN conda env create -f /environment.yml && conda clean -a
ENV PATH=/opt/conda/envs/lr-vcfmetrics/bin/:${PATH}
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/conda/envs/lr-sv/lib/

# Latex packages
RUN apt-get -y update \
	&& apt-get -y install git make cmake protobuf-compiler gcc g++ zlib1g-dev libcurl4-openssl-dev libbz2-dev tree python3-pip liblzma-dev wget curl \
    && apt-get install -y --no-install-recommends texlive-latex-recommended texlive-fonts-recommended \
    && apt-get install -y --no-install-recommends texlive-latex-extra texlive-fonts-extra texlive-lang-all \
	&& apt-get clean

RUN apt-get update && \
    rm -rf /var/lib/apt/lists/*

RUN echo "source activate lr-vcfmetrics" > ~/.bashrc
