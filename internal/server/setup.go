package server

import (
	"embed"
	"net/http"
)

//go:embed setup/claude.md setup/client.md
var setupGuides embed.FS

func registerSetupGuides(mux *http.ServeMux) {
	for _, name := range []string{"claude", "client"} {
		filename := "setup/" + name + ".md"
		mux.HandleFunc("GET /"+filename, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			http.ServeFileFS(w, r, setupGuides, filename)
		})
	}
}
