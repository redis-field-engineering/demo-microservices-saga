#################################################
GOOS	:= $(shell go env GOOS)
GOARCH	:= $(shell go env GOARCH)
GOFILES	:= $(shell ls *.go |grep -v test)
GOBUILD	:= GOOS=$(GOOS) GOARCH=$(GOARCH) go build


#################################################
default:	deps test build
docker:		deps test linuxbuild docker

deps:
	go get

build:
	$(GOBUILD) $(GOFILES)

linuxbuild:
	GOOS=linux GOARCH=amd64 go build $(GOFILES)

docker:
	docker build -t maguec/microservices-saga-backend .

test:
	go test -v
