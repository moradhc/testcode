package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/gocolly/colly"
	"github.com/joho/godotenv"
	"gopkg.in/mail.v2"
)

var wg sync.WaitGroup

var (
	smtpHost            string
	smtpPort            string
	senderEmailUsername string
	senderEmailPassword string
	emailSubject        string
)

type EmailData struct {
	RecruiterName  string
	JobTitle       string
	JobPortalUrl   string
	RecipientEmail string
}

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	smtpHost = os.Getenv("SMTP_HOST")
	smtpPort = os.Getenv("SMTP_PORT")
	senderEmailUsername = os.Getenv("SENDER_EMAIL_USERNAME")
	senderEmailPassword = os.Getenv("SENDER_EMAIL_PASSWORD")
	emailSubject = os.Getenv("EMAIL_SUBJECT")
}
func getJobLinks(url string) []string {
	c := colly.NewCollector()

	jobUrls := []string{}

	c.OnRequest(func(r *colly.Request) {
		log.Printf("Visiting %s\n", r.URL)
	})

	c.OnError(func(_ *colly.Response, err error) {
		log.Printf("Something went wrong: %s\n", err)
	})

	c.OnResponse(func(r *colly.Response) {
		if r.StatusCode == 200 {
			fmt.Println(200)
		}
	})

	c.OnHTML(".card__actions a", func(h *colly.HTMLElement) {
		jobUrls = append(jobUrls, h.Attr("href"))
	})

	c.OnHTML(".pagination__item.pagination__item--next a", func(h *colly.HTMLElement) {
		nextUrl := strings.TrimSpace(h.Attr("href"))
		if nextUrl != "" {
			h.Request.Visit(nextUrl)
		} else {
			fmt.Println("End")
		}
	})

	c.Visit(url)

	fmt.Println(jobUrls)
	return jobUrls

}

func sendEmail(emailData EmailData) {

	recipientEmail := emailData.RecipientEmail
	recruiterName := emailData.RecruiterName
	jobUrl := emailData.JobPortalUrl
	jobTitle := emailData.JobTitle

	body := fmt.Sprintf(`
	Good morning %s,

	I have seen your job offer "%s" on the job portal "%s".

	I have a wide experience in the sector, so for different roles I have been through. I am interested in this position to further improve my skills and languages.

	I will attach you my CV and anything else please contact me.

	Morad H.
	*** This email has been sent through an automated IA. ***
	%s

	`, recruiterName, jobTitle, jobUrl, senderEmailUsername)

	smtpPort, _ := strconv.Atoi(smtpPort)

	m := mail.NewMessage()
	m.SetHeader("From", senderEmailUsername)
	m.SetHeader("To", recipientEmail)
	m.SetHeader("Subject", emailSubject)
	m.SetBody("text/plain", body)

	fmt.Printf("Sending message to %s\n", recipientEmail)
	d := mail.NewDialer(smtpHost, smtpPort, senderEmailUsername, senderEmailPassword)
	d.StartTLSPolicy = mail.MandatoryStartTLS

	if err := d.DialAndSend(m); err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Printf("Email successfully sent to => %s", recipientEmail)

}

func getJobListingData(jobUrls []string) {

	var emailData []EmailData

	c := colly.NewCollector()

	c.OnRequest(func(r *colly.Request) {
		log.Printf("Visiting %s\n", r.URL)
	})

	c.OnError(func(_ *colly.Response, err error) {
		log.Printf("Something went wrong: %s\n", err)
	})

	c.OnResponse(func(r *colly.Response) {
		if r.StatusCode == 200 {
			fmt.Println(200)
		}
	})

	c.OnHTML("body", func(h *colly.HTMLElement) {

		if !strings.Contains(h.Text, "Oops, an error occurred!") && strings.Contains(h.Text, "Contact person:") {

			contactInfoArea := h.ChildText(".additional__text")
			contactData := strings.Split(contactInfoArea, ":")
			recruiterName := strings.TrimSpace(strings.Split(contactData[2], "E-mail")[0])
			email := strings.Split(strings.TrimSpace(strings.Split(contactData[3], "Attach")[0]), " ")[0]
			jobTitle := h.ChildText(".h3")
			jobUrl := h.Request.URL.String()

			data := EmailData{
				RecruiterName:  recruiterName,
				RecipientEmail: email,
				JobTitle:       jobTitle,
				JobPortalUrl:   jobUrl,
			}

			fmt.Println(data)
			emailData = append(emailData, data)
			sendEmail(data)
		}

	})

	for _, url := range jobUrls {
		wg.Add(1)
		c.Visit(url)
	}
	defer wg.Done()
}

func main() {

	fmt.Println("Enter the search keyword")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	keyword := scanner.Text()
	var joinedKeyword string
	splittedKeyword := strings.Split(keyword, " ")

	joinedKeyword = strings.Join(splittedKeyword, "+")

	url := "https://www.make-it-in-germany.com/en/working-in-germany/job-listings?tx_solr%5Bq%5D=" + joinedKeyword + "&%5Bfilter%5D%5B%5D=&%5Bfilter%5D%5B%5D=#filter45536"
	fmt.Println(url)
	jobUrls := getJobLinks(url)
	getJobListingData(jobUrls)
	wg.Wait()
}
