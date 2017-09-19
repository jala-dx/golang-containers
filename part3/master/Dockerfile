FROM golang:1.6-onbuild
  COPY . /go/src/app
  RUN go get -d -v
  RUN go install -v
  RUN echo "The result will be stored in the current folder, Please modify the yaml file to specify your own folder, in my case it is /Users/jganapat/go/src/part3 is mapped to containers /vol folder"

