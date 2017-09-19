package main

import (
  "fmt"
  "encoding/json"
  "net/http"
  "sync"
  "os"
  )

  


/*
 * Error Handler
 */
func check(e error) {
  if e != nil {
    panic(e)
  }
}

func ReadFile(fileid int, wgp *sync.WaitGroup, ch chan(map[string]int)) {

  url := fmt.Sprintf("http://rpc_slavesvc:12345/files/%d", fileid)

  fmt.Println("URL:", url)

  req, err := http.NewRequest("GET", url, nil)
  check(err)

  //A client is a http client
  client := &http.Client{}
  resp,err := client.Do(req)
  check(err)
  defer resp.Body.Close()

  // Use json.Decode for reading streams of JSON data

  dict := make( map[string]int )

  err = json.NewDecoder(resp.Body).Decode(&dict)
  check(err)
  fmt.Println("Printing dict...")
  fmt.Println(dict)
  ch <- dict
  wgp.Done()

}


/*
 * Input : Array of files, number of files in the array
 * Output: None
 * Desc  : finds the Frequency of words in the given files, assimilates the
 * output, and writes it to output.txt
 *
 */
func ReadFilesInParallel(numFiles int){

  var wg sync.WaitGroup
  wg.Add(numFiles)

  // Create a channel of maps, each channel will store the output map of each file
  ch := make (chan  (map[string]int), numFiles)
  for i := 0; i < numFiles; i ++ {
    go ReadFile(i, &wg, ch)
  }
  // Wait till all the goroutines complete
  wg.Wait()

  // Now all goroutines are complete, get the maps from channel and create
  // the final map
  fmt.Println("Final Result is stored in /vol/part1-output.txt")
  finalmap := make(map[string]int)
  for i := 0; i < numFiles; i ++ {
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
  f, err := os.Create("/vol/part3-output.txt")
  check(err)
  defer f.Close()
  for k, v := range finalmap {
    //fmt.Printf("Key:%v,  Value:%v\n", k, v)
    str := fmt.Sprintf("%-25s:%05d\n",k,v)
    _, err := f.WriteString(str)
    check(err)
  }
  f.Sync()
  fmt.Println("Finalmap:", finalmap)
}


func main() {

  numFiles := 135 
  ReadFilesInParallel(numFiles)
}




