VERSION = 1.0.13 # This should match the GraphAligner version number
TAG1 = quay.io/broad-long-read-pipelines/lr-graphaligner:$(VERSION)
TAG2 = quay.io/broad-long-read-pipelines/lr-graphaligner:latest
TAG3 = us.gcr.io/broad-dsp-lrma/lr-graphaligner:$(VERSION)
TAG4 = us.gcr.io/broad-dsp-lrma/lr-graphaligner:latest

all: build push

build:
	docker build -t $(TAG1) -t $(TAG2) -t $(TAG3) -t $(TAG4) .

push:
	docker push $(TAG1)
	docker push $(TAG2)
	docker push $(TAG3)
	docker push $(TAG4) 
