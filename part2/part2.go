package main

import (
  "fmt"
  "io/ioutil"
  "sync"
  "strings"
  "os"
  )
  

  

var replacer = strings.NewReplacer(";","",",","", "?","", ".","","!","",")","")

/*
 * Input : string, channel of Maps
 * Output: None
 * Desc  : Counts the occurence of word in a given string.
 * Assumption: Trims the trailing charcters - comma, "?", and ".", ';', !.
 *             We can add additional charcters to the above replacer.
 */
func wordCount(s string, ch chan(map[string]int)) {
  
  //fmt.Println("At wordCount for new file")
  dict := make( map[string]int )
  //array := split(s)
  array := strings.Fields(s)
  for _, word := range array {
    word := replacer.Replace(word)
    word = strings.ToLower(word)
    _, present := dict[word]
    //fmt.Println(val, present, word)
    if present {
      dict[word]++
      //fmt.Println("Incrementing..", "curr:", dict[word], word)
    }else {
      dict[word] = 1
      //fmt.Println("Adding new..", "curr:", dict[word], word)
    }
  }
  ch <- dict
  //fmt.Println("Exiting wordCount\n\n")
  
}

/*
 * Error Handler
 */
func check(e error) {
  if e != nil {
    panic(e)
  }
}

/*
 * Input : string, WaitGroup, channel of maps
 * Output: None
 * Desc  : Reads the content of given file, 
 * 
 */
func ReadFile(fileName string, wgp *sync.WaitGroup, ch chan(map[string]int)) {
  dat, err := ioutil.ReadFile(fileName)
  check(err)
  //fmt.Print(string(dat))
  wordCount(string(dat), ch)
  wgp.Done()
  
}

/*
 * Input : Array of files, number of files in the array
 * Output: None
 * Desc  : finds the Frequency of words in the given files, assimilates the
 * output, and writes it to output.txt
 * 
 */
func ReadFilesInParallel(arr_files []string, numFiles int) {
  
  var wg sync.WaitGroup
  wg.Add(numFiles)
  
  // Create a channel of maps, each channel will store the output map of each file
  ch := make (chan  (map[string]int), numFiles)
  for i := 0; i < numFiles; i ++ {
    go ReadFile(arr_files[i], &wg, ch)
    //ReadFile(arr_files[i], &wg, ch)
  }
  // Wait till all the goroutines complete
  wg.Wait()
  
  // Now all goroutines are complete, get the maps from channel and create 
  // the final map
  fmt.Println("Final Result is stored in /vol/part2-output.txt")
  finalmap := make(map[string]int)
  for i := 0; i < numFiles; i ++ {
    //fmt.Println(arr_files[i])
    dict := <-ch
    for k, _ := range dict {
      _, present := finalmap[k]
      if present {
        finalmap[k] += dict[k]
        //fmt.Println("Incrementing..", "curr:", dict[k], k)
      }else {
        //fmt.Println("Adding new..", "curr:", dict[k], k)
        finalmap[k] = dict[k]
      }
      //fmt.Printf("Key: %v Value: %v\n", k, v)
    }
  }
  //Write the results into file
  f, err := os.Create("/vol/part2-output.txt")
  check(err)
  defer f.Close()
  for k, v := range finalmap {
    //fmt.Printf("Key:%v,  Value:%v\n", k, v)
    str := fmt.Sprintf("%-25s:%05d\n",k,v)
    _, err := f.WriteString(str)
    check(err)
  }
  f.Sync()
}

/*
 * Input : Number of file names to be created.
 * Output: Array of strings
 * Desc  : finds the Frequency of words in the given files, assimilates the
 * output, and writes it to output.txt
 * 
 */
func createFileList(size int)  []string {
  arr := make([]string, size)
  fname := "/vol/moby-"
  for i := 0; i < size; i ++ {
    str := fmt.Sprintf("%v%03d.txt",fname,i)
    //fmt.Println(str)
    arr[i] = str
  }
  return arr
}

/*
 * main 
 */
func main() {
  ret_arr := createFileList(135)
  ReadFilesInParallel(ret_arr, 135)
}
