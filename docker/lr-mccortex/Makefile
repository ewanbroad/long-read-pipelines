VERSION = 1.0.0 # should match McCortex version number
TAG1 = quay.io/broad-long-read-pipelines/lr-mccortex:$(VERSION)
TAG2 = quay.io/broad-long-read-pipelines/lr-mccortex:latest
TAG3 = us.gcr.io/broad-dsp-lrma/lr-mccortex:$(VERSION)
TAG4 = us.gcr.io/broad-dsp-lrma/lr-mccortex:latest

all: build push

build:
	docker build -t $(TAG1) -t $(TAG2) -t $(TAG3) -t $(TAG4) .

push:
	docker push $(TAG1)
	docker push $(TAG2)
	docker push $(TAG3)
	docker push $(TAG4)
