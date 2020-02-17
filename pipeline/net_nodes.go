package pipeline

import (
	"bufio"
	"net"
)

//开一个server直接返回 开一个goroutine来等待连接 这样就可以保证在createpipeline时候不干活
func NetworkSink(addr string,in<-chan int)  {
	listener,err:=net.Listen("tcp",addr)
	if err!=nil{
		panic(err)
	}

	go func() {
		defer listener.Close()
		conn,err:=listener.Accept()
		if err!=nil{
			panic(err)
		}
		defer conn.Close()

		writer:=bufio.NewWriter(conn)
		WriteSink(writer,in)

	}()
}

func NetworkSource(addr string) <-chan int {
	out:=make(chan int)
	go func() {
		conn,err:=net.Dial("tcp",addr)
		if err!=nil{
			panic(err)
		}

		r:=ReadSource(bufio.NewReader(conn),-1)

		for v:= range r{
			out<-v
		}
		close(out)
	}()
	return out
}
