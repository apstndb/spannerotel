# spannerotel

## Usage

```go
import "cloud.google.com/go/spanner"
import "github.com/apstndb/spannerotel/interceptor"
import "google.golang.org/grpc"

// Setup some exporters
// ...
// interceptor
client, err := spanner.NewClientWithConfig(ctx, database, spanner.ClientConfig{
   // ...
}, option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(interceptor.StreamInterceptor(interceptor.WithDefaultDecorators()))),
)
```