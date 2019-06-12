package chinsert

import (
	"fmt"
)

//go:generate ldetool generate --package chinsert insertURL.lde

type Password string

func (p Password) String() string {
	return "★★★★★★"
}

// ConnParams client connection parameters
type ConnParams struct {
	Host     string
	Port     int
	User     string
	Password Password
	DBName   string
}

// String returns URL representation suitable for Open shortcut
func (c ConnParams) String() string {
	var auth string
	if len(c.User) > 0 {
		auth = c.User
	}
	if len(c.Password) > 0 {
		if len(c.User) == 0 {
			panic("got a password without a user")
		}
		auth += ":" + string(c.Password)
	}

	var url string
	if len(auth) > 0 {
		url = auth + "@"
	}

	if len(c.Host) > 0 {
		url += c.Host
	} else {
		url += "localhost"
	}
	if c.Port > 0 {
		url += fmt.Sprintf(":%d", c.Port)
	} else {
		url += ":8123"
	}

	if len(c.DBName) > 0 {
		url += fmt.Sprintf("/%s", c.DBName)
	}
	return url
}

// ParseURL retrieves ConnParams from Open compatible URL
func ParseURL(urlString string) (c ConnParams, err error) {
	p := &URL{}
	if _, err = p.Extract([]byte(urlString)); err != nil {
		return
	}
	if p.Auth.Valid {
		r := &Auth{}
		if _, err = r.Extract(p.GetAuthData()); err != nil {
			return
		}
		if len(r.User) == 0 {
			return c, fmt.Errorf("got empty user name")
		}
		c.User = string(r.User)
		if len(r.Password) > 0 {
			c.Password = Password(r.Password)
		}
	}
	c.Host = string(p.Host)
	c.Port = int(p.Port)
	if len(p.DBName) > 0 {
		c.DBName = string(p.DBName)
	}
	return
}
