FROM golang:1.6-onbuild
  COPY . /go/src/app
  RUN go get -d -v
  RUN go install -v
  RUN echo "Please create /vol folder and copy moby files into /vol folder before running the docker image"
