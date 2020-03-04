package main

import (
	"bufio"
	"fmt"
	"os"
	"pipeline/pipeline"
)

func main() {
	p:=creatPipeline("large.in",800000000,4)//修改fileSize大小就是修改large.out的大小
	writeToFile(p,"large.out")
	printFile("large.out")
}


func creatPipeline(filename string,fileSize,chunkCount int) <-chan int { //fileSize单位就是Byte

	chunkSize:=fileSize/chunkCount
	sortResults:=[]<-chan int{} //切片：chan int数组的视图

	pipeline.Init()

	for i:=0;i<chunkCount ; i++ {  //分块读取并排序
		file,err:=os.Open(filename)
		if err!=nil{
			panic(err)
		}

		_, _ = file.Seek(int64(i*chunkSize), 0)

		source:=pipeline.ReadSource(bufio.NewReader(file),chunkSize) //真正要分块读取文件啦

		sortResults=append(sortResults,pipeline.InMemSort(source))//把4个chan int拼起来
	}

	return pipeline.MergeN(sortResults...)  //要传slice的话需要先打散了
}

func writeToFile(p <-chan int, filename string) {
	file,err:=os.Create(filename)
	if err!=nil{
		panic(err)
	}
	defer file.Close()

	writer:=bufio.NewWriter(file)
	defer writer.Flush()

	pipeline.WriteSink(writer,p)

}

func printFile(filename string) {
	file,err :=os.Open(filename)
	if err!=nil{
		panic(err)
	}
	defer file.Close()

	p:=pipeline.ReadSource(file,-1)
	count:=0
	for v:=range p{
		fmt.Println(v)
		count++
		if count>=10{
			break
		}
	}
}






//todo 模拟网络节点
//func creatNetWorkPipeline(filename string,fileSize,chunkCount int) <-chan int { //fileSize单位就是Byte
//
//	chunkSize:=fileSize/chunkCount
//	sortAddr := []string
//
//	pipeline.Init()
//
//	for i:=0;i<chunkCount ; i++ {  //分块读取并排序
//		file,err:=os.Open(filename)
//		if err!=nil{
//			panic(err)
//		}
//
//		_, _ = file.Seek(int64(i*chunkSize), 0)
//
//		source:=pipeline.ReadSource(bufio.NewReader(file),chunkSize)
//
//		//sortResults=append(sortResults,pipeline.InMemSort(source))
//		addr:=":"+strconv.Itoa(7000+i)
//		pipeline.NetworkSink(addr,pipeline.InMemSort(source))//每个人做完InMemSort开一个server
//		sortAddr=append(sortAddr,addr)
//	}
//
//	sortResults := []<-chan int{}
//	for _,addr:=range sortAddr{
//		sortResults = append(sortResults, pipeline.NetworkSource(addr))
//	}
//
//	return pipeline.MergeN(sortResults...)  //要传slice的话需要先打散了
//}

