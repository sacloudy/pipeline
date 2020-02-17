# pipeline

### 项目简介
  使用go语言并行计算模拟多节点外部排序。  
  用一个800MB的随机整数的文件做待排文件，分4块“读入”4个channel中(channel未设缓冲区)，假设此时数据规模可以一次性装入内存，调用库函数分别将它们排序，然后把
  所有的channel append起来传给MergeN函数进行递归地归并，最后将返回的channel写入文件，并读取其前100个数据验证。  
  
  其中主要功能函数（分块读取，内排序，归并）都是 out:=make(chan int)   go function(){}  return out 的实现方式


