/*
(7.5 points) Top words for each rating
Please design and implement a PySpark program to pick up the top 10 words for each
rating. Some of the words such as ”great”, ”good” are more common in 5 star rating
comments than the 1 star rating comments.
Hint: you can use (RATING, WORD) pair as keys and count the frequency of such
pairs.
Your Python code should print out the top 10 common words for each rating comments
like follows:

$ spark-submit 2-wordranking.py
top 10 common words
1 star rating : __ __ __ ...
2 star rating : __ __ __ ...
3 star rating : __ __ __ ...
4 star rating : __ __ __ ...
5 star rating : __ __ __ ...
*/
package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

const HW_FILE = "../Amazon_Comments.csv"
const FILE_DELIM = '^'
const PRODUCT_ID = 0
const REVIEW_ID = 1
const REVIEW_TITLE = 2
const REVIEW_TIME = 3
const VERIFIED = 4
const REVIEW_CONTENT = 5
const REVIEW_RATING = 6

type Review struct {
	ProductID           string   `json:"ProductID, csv:0`
	ReviewID            string   `json:"ReviewID, csv:1`
	ReviewTitle         string   `json:"ReviewTitle, csv:2`
	ReviewTime          string   `json:"ReviewTime, csv:3`
	Verified            string   `json:"Verified, csv:4`
	ReviewContent       string   `json:"ReviewContent, csv:5`
	ReviewRating        int      `json:"ReviewRating, csv:6`
	ReviewContentTokens []string `json:"ReviewContentTokens", csv:5`
}

type RatingWordKey struct {
	ReviewRating int
	Word         string
}

var STOP_WORDS map[string]bool = map[string]bool{
	"":     true,
	"the":  true,
	"it":   true,
	"and":  true,
	"to":   true,
	"of":   true,
	"this": true,
	"not":  true,
	"was":  true,
	"but":  true,
	"my":   true,
	"for":  true,
	"is":   true,
	"with": true,
	"have": true,
	"as":   true,
	"you":  true,
	"in":   true,
	"on":   true,
	"so":   true,
	"that": true,
	"at":   true,
	"be":   true,
	"or":   true,
	"has":  true,
	"if":   true,
}

func main() {
	csvFile, _ := os.Open(HW_FILE)
	r := csv.NewReader(bufio.NewReader(csvFile))
	r.Comma = FILE_DELIM
	var maxCount int

	dict := make(map[RatingWordKey]int)

	for {
		line, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		rating, err := strconv.ParseFloat(line[REVIEW_RATING], 32)
		if err != nil {
			log.Fatal(err)
			rating = 0
		}

		review := Review{
			ProductID:     line[PRODUCT_ID],
			ReviewID:      line[REVIEW_ID],
			ReviewTitle:   line[REVIEW_TITLE],
			ReviewTime:    line[REVIEW_TITLE],
			Verified:      line[VERIFIED],
			ReviewContent: line[REVIEW_CONTENT],
			ReviewRating:  int(rating),
			ReviewContentTokens: filterString(mapString(
				strings.Split(
					strings.Map(removePunc, line[REVIEW_CONTENT]),
					" "),
				func(s string) string {
					return strings.ToLower(strings.TrimSpace(s))
				}),
				func(s string) bool {
					if len(s) <= 1 || STOP_WORDS[s] {
						return false
					}
					return true
				}),
		}

		for _, word := range review.ReviewContentTokens {
			dict[RatingWordKey{review.ReviewRating, word}]++

			if (maxCount < dict[RatingWordKey{review.ReviewRating, word}]) {
				maxCount = dict[RatingWordKey{review.ReviewRating, word}]
			}
		}
	}

	var ratingArray [][][]string = make([][][]string, 6)
	for ratingWordKey, count := range dict {
		if ratingArray[ratingWordKey.ReviewRating] == nil {
			ratingArray[ratingWordKey.ReviewRating] = make([][]string, maxCount+1)
			if ratingArray[ratingWordKey.ReviewRating][count] == nil {
				ratingArray[ratingWordKey.ReviewRating][count] = make([]string, 1)
			}
		}

		ratingArray[ratingWordKey.ReviewRating][count] = append(ratingArray[ratingWordKey.ReviewRating][count], ratingWordKey.Word)
	}

	for i := 5; i >= 1; i-- {
		var tenCommon []string
		count := 0
		for j := len(ratingArray[i]) - 1; j >= 0 && count < 10; j-- {
			for k := 0; k < len(ratingArray[i][j]) && count < 10; k++ {
				tenCommon = append(tenCommon, ratingArray[i][j][k])
				count++
			}
		}
		fmt.Println(i, " star rating : ", tenCommon)
	}
}

func removePunc(r rune) rune {
	switch {
	case r >= 'A' && r <= 'Z':
		fallthrough
	case r >= 'a' && r <= 'z':
		fallthrough
	case r >= '0' && r <= '9':
		return r
	}
	return ' '
}

func mapString(vs []string, f func(string) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

func filterString(vs []string, f func(string) bool) []string {
	var vsm []string
	for _, v := range vs {
		if f(v) {
			vsm = append(vsm, v)
		}
	}
	return vsm
}
