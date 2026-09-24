package postgresql

import (
	"github.com/Trendyol/go-mongo-cdc-postgresql/config"
	"gorm.io/gorm"
)

type SinkResponseHandlerContext struct {
	Action *Action
	Err    error
}

type SinkResponseHandlerInitContext struct {
	Config *config.Config
	DB     *gorm.DB
}

// SinkResponseHandler receives callbacks for each processed action.
type SinkResponseHandler interface {
	OnSuccess(ctx *SinkResponseHandlerContext)
	OnError(ctx *SinkResponseHandlerContext)
	OnInit(ctx *SinkResponseHandlerInitContext)
}
