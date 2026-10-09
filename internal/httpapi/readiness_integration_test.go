package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/Nuryanfa/TaskForge/internal/httpapi"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
)

func TestReadinessFailsAfterDatabasePoolClosesIntegration(t *testing.T) {
	databaseURL := os.Getenv("TASKFORGE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TASKFORGE_TEST_DATABASE_URL is not set")
	}
	store, err := postgres.Open(context.Background(), config.Config{
		DatabaseURL: databaseURL, DatabaseMaxConns: 2, DatabaseMinConns: 0,
		DatabaseConnectTimeout: 3 * time.Second, DatabaseQueryTimeout: time.Second,
		JobResultMaxBytes: 1024,
	})
	if err != nil {
		t.Fatal("open integration store")
	}
	store.Close()
	handler := httpapi.New(store, 1024, 100*time.Millisecond)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status=%d, want 503", response.Code)
	}
}
