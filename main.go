package main

import (
    "fmt"
    "log"
    "os"
    "bufio"
    "sync"
    "flag"
)

var kmer_size int
var kmer_counts = make(map[string]int)
var kmer_queue = make(chan string, 10000)
var wg sync.WaitGroup

func main() {
    kmerPtr := flag.Int("k", 27, "k-mer size")
    fastaPtr := flag.String("f", "", "fasta file")
    flag.Parse()

    kmer_size = *kmerPtr
    file, err := os.Open(*fastaPtr)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    i := 0
    go store_kmers()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        if (i % 4 == 1) {
            wg.Add(1)
            go get_kmers(scanner.Text())
        }

        i += 1
    }

    wg.Wait()

    for kmer := range kmer_counts {
        fmt.Printf("%s\n>%d\n", kmer, kmer_counts[kmer])
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }
}

func store_kmers() {
    for kmer := range kmer_queue {
        kmer_counts[kmer]++
    }
}

func get_kmers(read string) {
    length := len(read)
    
    for i := 0; i < length - kmer_size; i++ {
        kmer_queue <- read[i:i+kmer_size]
    }

    wg.Done()
}
