package main

import (
	"bufio"
	"fmt"
	"os"
	"pipeline/pipeline"
)


//创建文件，打开文件并读取文件        提示：reader代表可读，writer代表可写
func main() {

	//创建文件
	//const filename = "small.in"
	//const n=64
	const filename = "large.in"
	const n=100000000  //单位是int64      也就是64MB(61MB)

	file,err:=os.Create(filename)
    if err!=nil{
    	panic(err)
	}
	defer file.Close()
	p:=pipeline.RandomSource(n)


	writer:=bufio.NewWriter(file) //给文件加个buffer
	pipeline.WriteSink(writer,p)  //将channel p中的数据写入file(原来直接是file，没有buf，速度太慢)
	_ = writer.Flush()  //没有flush p中数据导入文件不彻底？

	//打开并读取文件前100行
	file,err=os.Open(filename)
	if err!=nil{
		panic(err)
	}
	defer file.Close()
	p=pipeline.ReadSource(bufio.NewReader(file),-1)//同样用bufio包装提高性能
	count:=0
	for v:= range p{
		fmt.Println(v)
		count++
		if count>=100{
			break
		}
	}
}


func mergeDemo(){
	p:=pipeline.Merge(pipeline.InMemSort(pipeline.ArraySource(1,6,88,8,9,7)),
		pipeline.InMemSort(pipeline.ArraySource(1,6,5,9,7)))

	for v:= range p{
		fmt.Println(v)
	}
}