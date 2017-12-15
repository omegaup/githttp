# githttp

[![Documentation](https://godoc.org/github.com/omegaup/githttp?status.svg)](https://godoc.org/github.com/omegaup/githttp)
[![Go Report Card](https://goreportcard.com/badge/github.com/omegaup/githttp)](https://goreportcard.com/report/github.com/omegaup/githttp)

A Go implementation of Git's HTTP "smart" protocol.

Minimalistic example with git bare repositories (with the `.git` extension) in
the `git_repositories/` directory:

```go
package main

import (
        "github.com/inconshreveable/log15"
        "github.com/omegaup/githttp"
        "net/http"
)

func main() {
        panic(http.Server{
                Addr:    ":80",
                Handler: githttp.GitServer("git_repositories", true, nil, nil, nil, log15.New()),
        }.ListenAndServe())
}
```
