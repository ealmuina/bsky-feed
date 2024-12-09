#! /bin/bash

# Download fasttext language detection model
if ! [ -e "/app/utils/lid.176.bin" ] ; then
    curl -o /app/utils/lid.176.bin https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin
fi

# Start server
go run .
