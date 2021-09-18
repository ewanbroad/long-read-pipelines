FROM continuumio/miniconda3

MAINTAINER Kiran V Garimella

# copy other resources
COPY ./environment.yml /

# install conda packages
RUN conda env create -f /environment.yml && conda clean -a
ENV PATH=/opt/conda/envs/lr-graphaligner/bin/:${PATH}

# install utilities
RUN apt update
RUN apt install -y curl git-lfs time datamash

# activate conda environment
RUN echo "source activate lr-graphaligner" > ~/.bashrc
