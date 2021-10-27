FROM continuumio/miniconda3

MAINTAINER Kiran V Garimella

# copy other resources
COPY ./environment.yml /

# install conda packages
RUN conda env create -f /environment.yml && conda clean -a
ENV PATH=/opt/conda/envs/lr-mccortex/bin/:${PATH}
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/conda/envs/lr-mccortex/lib/

#RUN apt-get -y update \
#    && apt-get -y install tree \
#    && apt-get clean

RUN git clone https://github.com/mcveanlab/Corticall \
    && cd Corticall \
    && ant \
    && mv dist/corticall.jar /

RUN echo "source activate lr-mccortex" > ~/.bashrc
