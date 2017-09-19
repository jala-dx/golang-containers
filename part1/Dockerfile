FROM golang:1.6-onbuild
  COPY . /go/src/app
  RUN go get -d -v
  RUN go install -v
  RUN echo "When instantiating this container please create /vol folder - where the result file will be stored"
