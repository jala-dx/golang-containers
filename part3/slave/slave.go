package main

import (
  "fmt"
  "encoding/json"
  "log"
  "net/http"
  "github.com/gorilla/mux"
  "io/ioutil"
  "sync"
  "strings"
  "strconv"
  )


func GetAllFiles(w http.ResponseWriter, req *http.Request) {
    // TODO nice to have
}

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


func ReadFile(filename string, wgp *sync.WaitGroup, ch chan(map[string]int)) {
  dat := get_moby(filename)
  wordCount(dat, ch)
  wgp.Done()

}

/*
 * Input : Array of files, number of files in the array
 * Output: None
 * Desc  : finds the Frequency of words in the given files, assimilates the
 * output, and writes it to output.txt
 *
 */
func ReadFilesInParallel(arr_files []string, numFiles int) map[string]int {

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
  return finalmap

}



func createFileList(fileid int, size int)  []string {

  fmt.Println("Entering createFileList ...")
  arr := make([]string, size)
  fname := "moby-"
  j := 0
  sz := fileid + size
  fmt.Println(sz)

  for i := fileid; i < sz; i ++ {
    str := fmt.Sprintf("%v%03d.txt",fname,i)
    arr[j] = str
    j += 1
  }
  fmt.Println(arr)
  return arr
}


func get_moby(filename string) string {

    url := "http://www.gutenberg.org/files/15/text/" + filename
    fmt.Println(url)
    
    resp, err := http.Get(url)
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()
    html, err := ioutil.ReadAll(resp.Body)
    if err !=nil {
        panic(err)
    }
    return(string(html))

}

func GetFile(w http.ResponseWriter, req *http.Request) {
    params  := mux.Vars(req)
    pid     := params["id"]
    fmt.Println(pid)
    id, err := strconv.Atoi(pid)
    check(err)
    flist  := createFileList(id, 1)
    fmt.Println(flist)
    finalmap := ReadFilesInParallel(flist, 1)
    json.NewEncoder(w).Encode(finalmap)
}

func GetFilesRange(w http.ResponseWriter, req *http.Request) {
    params  := mux.Vars(req)
    pid1     := params["id1"]
    pid2     := params["id2"]
    fmt.Println(pid1)
    fmt.Println(pid2)
    id1, err := strconv.Atoi(pid1)
    check(err)
    id2, err := strconv.Atoi(pid2)
    check(err)
    sz := (id2 - id1) + 1
    flist  := createFileList(id1,sz)
    finalmap := ReadFilesInParallel(flist, sz)
    json.NewEncoder(w).Encode(finalmap)
}

func main() {

  router := mux.NewRouter()
  router.HandleFunc("/files", GetAllFiles).Methods("GET")
  router.HandleFunc("/files/{id1}-{id2}", GetFilesRange).Methods("GET")
  router.HandleFunc("/files/{id}", GetFile).Methods("GET")
  log.Fatal(http.ListenAndServe(":12345", router))
}




