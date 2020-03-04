package pipeline

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"
)
var startTime time.Time

func Init()  {
	startTime=time.Now()
}

//将channel中数据读入内存排序后返回一个channel
func InMemSort(in <-chan int) chan int {
	out := make(chan int,1024)
	go func() {

		a:=[]int{}
		for v:=range in{
			a=append(a,v)
		}

		sort.Ints(a)
		fmt.Println("InMemSort done:",time.Now().Sub(startTime))

		for _,v:=range a {
			out<-v
		}
		close(out)
	}()
	return out
}

func MergeN(inputs ...<-chan int) <-chan int{
	if len(inputs)==1{
		return inputs[0]
	}
	m:=len(inputs)/2
	return Merge(MergeN(inputs[:m]...),MergeN(inputs[m:]...))//啊！这也是瞬间就返回了！！！
}

func Merge(in1,in2 <-chan int) <-chan int {
	out:=make(chan int,1024)
	go func() {
		v1,ok1:=<-in1
		v2,ok2:=<-in2
		for ok1 || ok2{
			if !ok2 || (ok1 && v1<=v2){
				out<-v1
				v1,ok1=<-in1
			}else{
				out<-v2
				v2,ok2=<-in2
			}
		}
		close(out)
		fmt.Println("Merge done:",time.Now().Sub(startTime))
	}()
	return out
}

//将可读的reader中的数据读一个chunk到一个channel中
func ReadSource(reader io.Reader,chunkSize int) <-chan int { //要求传入一个实现了可读接口的struct

	out := make(chan int,1024)
	go func() {
		buffer:=make([]byte,8)     //要用buffer的
		bytesRead := 0
		for{
			n,err:=reader.Read(buffer) //读reader到buffer
			bytesRead+=n
			if n>0{    //就算有n>0 err也可能有错误(读到EOF了)
				v:=int(binary.BigEndian.Uint64(buffer)) //强转成有符号的int   大端
				out<-v
			}
			if err!=nil ||(chunkSize!=-1&&bytesRead>=chunkSize){  //传入-1就代表不进行分块读
				break
			}
		}
		fmt.Println("Read done:",time.Now().Sub(startTime))
		close(out)
	}()
	return out
}

//传入的file文件实现了Write接口 所以拥有了 被写入 功能
func WriteSink(writer io.Writer,in <-chan int)  { //要求传入的参数必须实现了 可以被写入 接口
	for v:= range in{
		buffer:=make([]byte,8)    //控制数据流动的单位
		binary.BigEndian.PutUint64(buffer,uint64(v))
		writer.Write(buffer)       //这是真正用到了接口的功能    //有个问题先留一留这不会归并排序返回的goroutine不是一下就返回了吗
	}
}

func RandomSource(count int) <-chan int{
	out := make(chan int)
	go func() {
		for i:=0;i<count;i++{
			out<-rand.Int()
		}
		close(out)
	}()
	return out
}

func ArraySource(a ...int) <-chan int {
	out:=make(chan int)
	go func() {
		for _,v:=range a{
			out<-v
		}
		close(out)//一般在并发编程中不需要close，在这里并行计算需要
	}()
	return out
}
