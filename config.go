package redis

type RedisConfig struct {
	Addr     string `env:"ADDRESS,required,notEmpty"`
	Password string `env:"PASS,required,notEmpty"`
	User     string `env:"USER"`
}
