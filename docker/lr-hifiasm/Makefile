VERSION = 0.16.0
TAG1 = us.gcr.io/broad-dsp-lrma/lr-hifiasm:$(VERSION)
TAG2 = us.gcr.io/broad-dsp-lrma/lr-hifiasm:latest

all: build push

build:
	docker build -t $(TAG1) -t $(TAG2) .

push:
	docker push $(TAG1)
	docker push $(TAG2)
