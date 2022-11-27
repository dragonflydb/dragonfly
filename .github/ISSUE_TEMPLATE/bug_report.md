---
name: Bug report
about: Create a report to help Dragonfly DB improve
title: ''
labels: 'bug'
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Insert records using `command`
2. Query records using `command`
3. Scroll down to '....'
4. See error

**Expected behavior**
A clear and concise description of what you expected to happen.

**Screenshots**
If applicable, add screenshots to help explain your problem.

**Environment (please complete the following information):**
 - OS: [ubuntu 20.04]
 - Kernel: # Command: `uname -a`
 - Containerized?: [Bare Metal, Docker, Docker Compose, Docker Swarm, Kubernetes, Other]
 - Dragonfly Version: [e.g. 0.3.0]

**Reproducible Code Snippet**
```
# Minimal code snippet to reproduce this bug
```

**Additional context**
Add any other context about the problem here.
Skip to content
Search or jump to…
Pull requests
Issues
Codespaces
Marketplace
Explore
 
@zakwarlord7 
Your account has been flagged.
Because of that, your profile is hidden from the public. If you believe this is a mistake, contact support to have your account status reviewed.
zakwarlord7
/
Skip-to-content-Search-or-jump-to-Pull-requests-Issues-Codespaces-Marketplace-Explore-zakwarlord
Public
Code
Issues
Pull requests
Actions
Projects
Wiki
Security
Insights
Settings
Update README.md
 paradice
@zakwarlord7
zakwarlord7 committed 1 hour ago 
1 parent 483c37d commit 96c1a802360fea625062f190258edf5392cb1e13
Showing 1 changed file with 649 additions and 294 deletions.
  943  
README.md
@@ -1,174 +1,494 @@
$$22,662,983,361,013.70		Report Range:				Tips																										
	$215,014.49		Name: ZACHRY T. WOOD SSN: 13				$0.00																										
	0		Payment Summary																														
	22,662,983,361,014		Salary		Vacation hourly		OT																										
	$0.00		$3,361,013.70																														
	$8,385,561,229,657.00		Bonus		$0.00		$0.00																										
	$13,330.90		$0.00		Other Wages 1		Other Wages 2																										
	532,580,113,436		Total		$0.00		$0.00																										
	0		22,662,983,361,014																														
	0		Deduction Summary																														
	Amount		Health Insurance																														
	$13,330.90		$0.00																														
	328,613,309,009		Tax Summary																														
	$441.70		Federal Tax	$7.00			Total Tax																										
	$840.00		$8,385,561,229,657@3,330.90		Local Tax																												
					$0.00																												
	$0.00		Advanced EIC Payment			$8,918,141,356,423.43																											
	$0.00		$0.00				Total																										
					401K																												
					$0.00		$0.00																										

						Social Security Tax Medicare TaxState Tax																											
							$532,580,113,050)																										




				$9,246,754,678,763.00																													





			Q4 2021	Q3 2021	Q2 2021	Q1 2021	Q4 2020																										

			$24,934,000,000.00	$25,539,000,000.00	$37,497,000,000.00	$31,211,000,000.00	$30,818,000,000.00																										
			$24,934,000,000.00	$25,539,000,000.00	$21,890,000,000.00	$19,289,000,000.00	$22,677,000,000.00																										
			$24,934,000,000.00	$25,539,000,000.00	$21,890,000,000.00	$19,289,000,000.00	$22,677,000,000.00																										
			$20,642,000,000.00	$18,936,000,000.00	$18,525,000,000.00	$17,930,000,000.00	$15,227,000,000.00																										
			$6,517,000,000.00	$3,797,000,000.00	$4,236,000,000.00	$2,592,000,000.00	$5,748,000,000.00																										
			$3,439,000,000.00	$3,304,000,000.00	$2,945,000,000.00	$2,753,000,000.00	$3,725,000,000.00																										
			$3,439,000,000.00	$3,304,000,000.00	$2,945,000,000.00	$2,753,000,000.00	$3,725,000,000.00																										
			$3,215,000,000.00	$3,085,000,000.00	$2,730,000,000.00	$2,525,000,000.00	$3,539,000,000.00																										
			$224,000,000.00	$219,000,000.00	$215,000,000.00	$228,000,000.00	$186,000,000.00																										
			$3,954,000,000.00	$3,874,000,000.00	$3,803,000,000.00	$3,745,000,000.00	$3,223,000,000.00																										
			$1,616,000,000.00	-$1,287,000,000.00	$379,000,000.00	$1,100,000,000.00	$1,670,000,000.00																										
			-$2,478,000,000.00	-$2,158,000,000.00	-$2,883,000,000.00	-$4,751,000,000.00	-$3,262,000,000.00																										
			-$2,478,000,000.00	-$2,158,000,000.00	-$2,883,000,000.00	-$4,751,000,000.00	-$3,262,000,000.00																										
			-$14,000,000.00	$64,000,000.00	-$8,000,000.00	-$255,000,000.00	$392,000,000.00																										
			-$2,225,000,000.00	$2,806,000,000.00	-$871,000,000.00	-$1,233,000,000.00	$1,702,000,000.00																										
			-$5,819,000,000.00	-$2,409,000,000.00	-$3,661,000,000.00	$2,794,000,000.00	-$5,445,000,000.00																										
			-$5,819,000,000.00	-$2,409,000,000.00	-$3,661,000,000.00	$2,794,000,000.00	-$5,445,000,000.00																										
			-$399,000,000.00	-$1,255,000,000.00	-$199,000,000.00	$7,000,000.00	-$738,000,000.00																										
			$6,994,000,000.00	$3,157,000,000.00	$4,074,000,000.00	-$4,956,000,000.00	$6,938,000,000.00																										
			$1,157,000,000.00	$238,000,000.00	-$130,000,000.00	-$982,000,000.00	$963,000,000.00																										
			$1,157,000,000.00	$238,000,000.00	-$130,000,000.00	-$982,000,000.00	$963,000,000.00																										
			$5,837,000,000.00	$2,919,000,000.00	$4,204,000,000.00	-$3,974,000,000.00	$5,975,000,000.00																										
			$368,000,000.00	$272,000,000.00	-$3,000,000.00	$137,000,000.00	$207,000,000.00																										
			-$3,369,000,000.00	$3,041,000,000.00	-$1,082,000,000.00	$785,000,000.00	$740,000,000.00																										


			-$11,016,000,000.00	-$10,050,000,000.00	-$9,074,000,000.00	-$5,383,000,000.00	-$7,281,000,000.00																										
			-$11,016,000,000.00	-$10,050,000,000.00	-$9,074,000,000.00	-$5,383,000,000.00	-$7,281,000,000.00																										

			-$6,383,000,000.00	-$6,819,000,000.00	-$5,496,000,000.00	-$5,942,000,000.00	-$5,479,000,000.00																										
			-$6,383,000,000.00	-$6,819,000,000.00	-$5,496,000,000.00	-$5,942,000,000.00	-$5,479,000,000.00																										

			-$385,000,000.00	-$259,000,000.00	-$308,000,000.00	-$1,666,000,000.00	-$370,000,000.00																										
			-$385,000,000.00	-$259,000,000.00	-$308,000,000.00	-$1,666,000,000.00	-$370,000,000.00																										
			-$4,348,000,000.00	-$3,360,000,000.00	-$3,293,000,000.00	$2,195,000,000.00	-$1,375,000,000.00																										
			-$40,860,000,000.00	-$35,153,000,000.00	-$24,949,000,000.00	-$37,072,000,000.00	-$36,955,000,000.00																										

			$36,512,000,000.00	$31,793,000,000.00	$21,656,000,000.00	$39,267,000,000.00	$35,580,000,000.00																										
			$100,000,000.00	$388,000,000.00	$23,000,000.00	$30,000,000.00	-$57,000,000.00																										

				-$15,254,000,000.00																													
			-$16,511,000,000.00	-$15,254,000,000.00	-$15,991,000,000.00	-$13,606,000,000.00	-$9,270,000,000.00																										
			-$16,511,000,000.00	-$12,610,000,000.00	-$15,991,000,000.00	-$13,606,000,000.00	-$9,270,000,000.00																										
			-$13,473,000,000.00	-$12,610,000,000.00	-$12,796,000,000.00	-$11,395,000,000.00	-$7,904,000,000.00																										
			$13,473,000,000.00		-$12,796,000,000.00	-$11,395,000,000.00	-$7,904,000,000.00																										
				-$42,000,000.00																													
			$115,000,000.00	-$42,000,000.00	-$1,042,000,000.00	-$37,000,000.00	-$57,000,000.00																										
			$115,000,000.00	$6,350,000,000.00	-$1,042,000,000.00	-$37,000,000.00	-$57,000,000.00																										
			$6,250,000,000.00	-$6,392,000,000.00	$6,699,000,000.00	$900,000,000.00	$0.00																										
			$6,365,000,000.00	-$2,602,000,000.00	-$7,741,000,000.00	-$937,000,000.00	-$57,000,000.00																										

			$2,923,000,000.00		-$2,453,000,000.00	-$2,184,000,000.00	-$1,647,000,000.00																										

			$0.00		$300,000,000.00	$10,000,000.00	$338,000,000,000.00																										


			$20,945,000,000.00	$23,719,000,000.00	$23,630,000,000.00	$26,622,000,000.00	$26,465,000,000.00																										
			$25,930,000,000.00	$235,000,000,000.00	-$3,175,000,000.00	$300,000,000.00	$6,126,000,000.00																										
			$181,000,000,000.00	$146,000,000,000.00																													
		$62.50		Reverse Corporate ACH Debit Effective 04-26-22			Reference number																										
$$22,662,983,361,013.70		Report Range:				Tips										


$215,014.49		Name: ZACHRY T. WOOD SSN: 13				$0.00									


0		Payment Summary																


22,662,983,361,014		Salary		Vacation hourly		OT										


$0.00		$3,361,013.70																


$8,385,561,229,657.00		Bonus		$0.00		$0.00											


$13,330.90		$0.00		Other Wages 1		Other Wages 2										


532,580,113,436		Total		$0.00		$0.00												


0		22,662,983,361,014															


0		Deduction Summary															


Amount		Health Insurance															


$13,330.90		$0.00																


328,613,309,009		Tax Summary															


$441.70		Federal Tax	$7.00			Total Tax											


$840.00		$8,385,561,229,657@3,330.90		Local Tax											


$0.00																			


$0.00		Advanced EIC Payment			$8,918,141,356,423.43										


$0.00		$0.00				Total													


401K																			


$0.00		$0.00																	




Social Security Tax Medicare TaxState Tax														


$532,580,113,050)																	










$9,246,754,678,763.00																	












Q4 2021	Q3 2021	Q2 2021	Q1 2021	Q4 2020																										



$24,934,000,000.00	$25,539,000,000.00	$37,497,000,000.00	$31,211,000,000.00	$30,818,000,000.00					


$24,934,000,000.00	$25,539,000,000.00	$21,890,000,000.00	$19,289,000,000.00	$22,677,000,000.00					


$24,934,000,000.00	$25,539,000,000.00	$21,890,000,000.00	$19,289,000,000.00	$22,677,000,000.00					


$20,642,000,000.00	$18,936,000,000.00	$18,525,000,000.00	$17,930,000,000.00	$15,227,000,000.00					


$6,517,000,000.00	$3,797,000,000.00	$4,236,000,000.00	$2,592,000,000.00	$5,748,000,000.00					


$3,439,000,000.00	$3,304,000,000.00	$2,945,000,000.00	$2,753,000,000.00	$3,725,000,000.00					


$3,439,000,000.00	$3,304,000,000.00	$2,945,000,000.00	$2,753,000,000.00	$3,725,000,000.00					


$3,215,000,000.00	$3,085,000,000.00	$2,730,000,000.00	$2,525,000,000.00	$3,539,000,000.00					


$224,000,000.00	$219,000,000.00	$215,000,000.00	$228,000,000.00	$186,000,000.00										


$3,954,000,000.00	$3,874,000,000.00	$3,803,000,000.00	$3,745,000,000.00	$3,223,000,000.00					


$1,616,000,000.00	-$1,287,000,000.00	$379,000,000.00	$1,100,000,000.00	$1,670,000,000.00						


-$2,478,000,000.00	-$2,158,000,000.00	-$2,883,000,000.00	-$4,751,000,000.00	-$3,262,000,000.00					


-$2,478,000,000.00	-$2,158,000,000.00	-$2,883,000,000.00	-$4,751,000,000.00	-$3,262,000,000.00					


-$14,000,000.00	$64,000,000.00	-$8,000,000.00	-$255,000,000.00	$392,000,000.00									


-$2,225,000,000.00	$2,806,000,000.00	-$871,000,000.00	-$1,233,000,000.00	$1,702,000,000.00					


-$5,819,000,000.00	-$2,409,000,000.00	-$3,661,000,000.00	$2,794,000,000.00	-$5,445,000,000.00					


-$5,819,000,000.00	-$2,409,000,000.00	-$3,661,000,000.00	$2,794,000,000.00	-$5,445,000,000.00					


-$399,000,000.00	-$1,255,000,000.00	-$199,000,000.00	$7,000,000.00	-$738,000,000.00						


$6,994,000,000.00	$3,157,000,000.00	$4,074,000,000.00	-$4,956,000,000.00	$6,938,000,000.00					


$1,157,000,000.00	$238,000,000.00	-$130,000,000.00	-$982,000,000.00	$963,000,000.00							


$1,157,000,000.00	$238,000,000.00	-$130,000,000.00	-$982,000,000.00	$963,000,000.00							


$5,837,000,000.00	$2,919,000,000.00	$4,204,000,000.00	-$3,974,000,000.00	$5,975,000,000.00					


$368,000,000.00	$272,000,000.00	-$3,000,000.00	$137,000,000.00	$207,000,000.00										


-$3,369,000,000.00	$3,041,000,000.00	-$1,082,000,000.00	$785,000,000.00	$740,000,000.00							






-$11,016,000,000.00	-$10,050,000,000.00	-$9,074,000,000.00	-$5,383,000,000.00	-$7,281,000,000.00					


-$11,016,000,000.00	-$10,050,000,000.00	-$9,074,000,000.00	-$5,383,000,000.00	-$7,281,000,000.00					




-$6,383,000,000.00	-$6,819,000,000.00	-$5,496,000,000.00	-$5,942,000,000.00	-$5,479,000,000.00					


-$6,383,000,000.00	-$6,819,000,000.00	-$5,496,000,000.00	-$5,942,000,000.00	-$5,479,000,000.00					




-$385,000,000.00	-$259,000,000.00	-$308,000,000.00	-$1,666,000,000.00	-$370,000,000.00					


-$385,000,000.00	-$259,000,000.00	-$308,000,000.00	-$1,666,000,000.00	-$370,000,000.00					


-$4,348,000,000.00	-$3,360,000,000.00	-$3,293,000,000.00	$2,195,000,000.00	-$1,375,000,000.00					


-$40,860,000,000.00	-$35,153,000,000.00	-$24,949,000,000.00	-$37,072,000,000.00	-$36,955,000,000.00					




$36,512,000,000.00	$31,793,000,000.00	$21,656,000,000.00	$39,267,000,000.00	$35,580,000,000.00					


$100,000,000.00	$388,000,000.00	$23,000,000.00	$30,000,000.00	-$57,000,000.00										




-$15,254,000,000.00																	


-$16,511,000,000.00	-$15,254,000,000.00	-$15,991,000,000.00	-$13,606,000,000.00	-$9,270,000,000.00					


-$16,511,000,000.00	-$12,610,000,000.00	-$15,991,000,000.00	-$13,606,000,000.00	-$9,270,000,000.00					


-$13,473,000,000.00	-$12,610,000,000.00	-$12,796,000,000.00	-$11,395,000,000.00	-$7,904,000,000.00					


$13,473,000,000.00		-$12,796,000,000.00	-$11,395,000,000.00	-$7,904,000,000.00							


-$42,000,000.00																		


$115,000,000.00	-$42,000,000.00	-$1,042,000,000.00	-$37,000,000.00	-$57,000,000.00									


$115,000,000.00	$6,350,000,000.00	-$1,042,000,000.00	-$37,000,000.00	-$57,000,000.00								


$6,250,000,000.00	-$6,392,000,000.00	$6,699,000,000.00	$900,000,000.00	$0.00								


$6,365,000,000.00	-$2,602,000,000.00	-$7,741,000,000.00	-$937,000,000.00	-$57,000,000.00						




$2,923,000,000.00		-$2,453,000,000.00	-$2,184,000,000.00	-$1,647,000,000.00							




$0.00		$300,000,000.00	$10,000,000.00	$338,000,000,000.00											






$20,945,000,000.00	$23,719,000,000.00	$23,630,000,000.00	$26,622,000,000.00	$26,465,000,000.00					


$25,930,000,000.00	$235,000,000,000.00	-$3,175,000,000.00	$300,000,000.00	$6,126,000,000.00						


$181,000,000,000.00	$146,000,000,000.00														


$62.50		Reverse Corporate ACH Debit Effective 04-26-22			Reference number							


"PLEASE READ THE IMPORTANT DISCLOSURES BELOW

                                                                                                                                                                                   COD                                : 633441725                                


COD                                : 633441725                                

CIF Department (Online Banking) Business Checking's Account: 47-2041-6547

P7-PFSC-04-F Business Type: Sole Proprietorship/Partnership Corporation

500 First Avenue ALPHABET

Pittsburgh, PA 15219-3128 5323 BRADFORD DR

NON-NEGOTIABLE DALLAS TX 75235 8313

ZACHRY, TYLER, WOOD

4/18/2022 650-2530-000 469-697-4300

SIGNATURE Time Zone: Eastern Central Mountain Pacific
Investment Products  • Not FDIC Insured  • No Bank Guarantee  • May Lose Value" "($143,000,000.00) $210,000,000.00 $23,719,000,000,000.00 $26,622,000,000,000.00 $26,465,000,000,000.00 $20,129,000,000,000.00 $2,774,000,000.00 $89,000,000.00 $2,992,000,000.00 $6,336,000,000.00 $13,412,000,000.00 $157,000,000.00 $4,990,000,000.00 Q4 2020Q4 2019 Dec. 31, 2020Dec. 31, 2019 $182,527.00 $161,857.00 $84,732.00 $71,896.00 $27,573.00 $26,018.00 $17,946.00 $18,464.00 $11,052.00 $9,551.00 $0.00 $1,697.00 $141,303.00 $127,626.00 $41,224.00 $34,231.00 $6,858,000,000.00 $5,394.00 $22,677,000,000.00 $19,289,000,000.00 $22,677,000,000.00 $19,289,000,000.00 $22,677,000,000.00 $19,289,000,000.00 $20,210,418.00 RateUnitsTotalYTDTaxes / DeductionsCurrentYTD --$70,842,745,000.00 $70,842,745,000.00 Federal Withholding$0.00 $0.00 FICA - Social Security$0.00 $8,853.60 FICA - Medicare$0.00 $0.00 Employer Taxes FUTA$0.00 $0.00 SUTA$0.00 $0.00 EIN: 61-1767919 SSN: 633441725 Gross $70,842,745,000.00 Earnings Statement Taxes / DeductionsStub Number: 1 $0.00 Net PaySSNPay SchedulePay PeriodSep 28, 2022 to Sep 29, 2023Pay Date$44,669.00 $70,842,745,000.00 XXX-XX-1725Annually CHECK NUMBER 221116905560140 $76,033,000,000.00 $20,642,000,000.00 $18,936,000,000.00 $18,525,000,000.00 $17,930,000,000.00 $15,227,000,000.00 $11,247,000,000.00 $6,959,000,000.00 $6,836,000,000.00 $10,671,000,000.00 $7,068,000,000.00 $76,033,000,000.00 $20,642,000,000.00 $18,936,000,000.00 $18,525,000,000.00 $17,930,000,000.00 $15,227,000,000.00 $11,247,000,000.00 $6,959,000,000.00 $6,836,000,000.00 $10,671,000,000.00 $7,068,000,000.00 $76,033,000,000.00 $20,642,000,000.00 $18,936,000,000.00 $18,525,000,000.00 $17,930,000,000.00 $15,227,000,000.00 $11,247,000,000.00 $6,959,000,000.00 $6,836,000,000.00 $10,671,000,000.00 $7,068,000,000.00 $76,033,000,000.00 $20,642,000,000.00 $18,936,000,000.00 $257,637,000,000.00 $75,325,000,000.00 $65,118,000,000.00 $61,880,000,000.00 $55,314,000,000.00 $56,898,000,000.00 $46,173,000,000.00 $38,297,000,000.00 $41,159,000,000.00 $46,075,000,000.00 $40,499,000,000.00 $78,714,000,000.00 $21,885,000,000.00 $21,031,000,000.00 $19,361,000,000.00 $16,437,000,000.00 $15,651,000,000.00 $11,213,000,000.00 $6,383,000,000.00 $7,977,000,000.00 $9,266,000,000.00 $9,177,000,000.00 $0.16 $0.18 $0.16 $0.16 $0.16 $0.16 $0.12 $0.18 $6,836,000,000.00 $7,977,000,000.00 $113.88 $31.15 $28.44 $27.69 $26.63 $22.54 $16.55 $10.21 $9.96 $15.49 $10.20 $113.88 $31.12 $28.44 $27.69 $26.63 $22.46 $16.55 $10.21 $9.96 $15.47 $10.20 $112.20 $30.69 $27.99 $27.26 $26.29 $22.30 $16.40 $10.13 $9.87 $15.35 $10.12 $112.20 $30.67 $27.99 $27.26 $26.29 $22.23 $16.40 $10.13 $9.87 $15.33 $10.12 $667,650,000.00 $662,664,000.00 $665,758,000.00 $668,958,000.00 $673,220,000.00 $675,581,000.00 $679,449,000.00 $681,768,000.00 $686,465,000.00 $688,804,000.00 $692,741,000.00 $677,674,000.00 $672,493,000.00 $676,519,000.00 $679,612,000.00 $682,071,000.00 $682,969,000.00 $685,851,000.00 $687,024,000.00 $692,267,000.00 $695,193,000.00 $698,199,000.00 $9.87 $113.88 $31.15 $28.44 $27.69 $26.63 $22.54 $16.55 $10.21 $9.96 $15.49 $10.20 $1.00 $112.20 $30.69 $27.99 $27.26 $26.29 $22.30 $16.40 $10.13 $9.87 $15.35 $10.12 $667,650,000.00 $662,664,000.00 $665,758,000.00 $668,958,000.00 $673,220,000.00 $675,581,000.00 $679,449,000.00 $681,768,000.00 $686,465,000.00 $688,804,000.00 $692,741,000.00 $677,674,000.00 $672,493,000.00 $676,519,000.00 $679,612,000.00 $682,071,000.00 $682,969,000.00 $685,851,000.00 $687,024,000.00 $692,267,000.00 $695,193,000.00 $698,199,000.00 Amount Transaction descriptionFor the period 04/13/2022 to 04/29/2022 ZACHRY TYLER WOOD Primary account number: 47-2041-6547 Page 2 of 3"" $22,116,905,560,149.00 Reference number Amount Transaction description $22,116,905,560,149.00 $62.50 Corporate ACH Quickbooks 180041ntuit 1940868 Reference number $22,116,905,560,149.00 Amount Transaction description on your next statement as a single line item entitled Service Waived - New Customer Period $36.00 Returned Item Fee (nsf)

						Amount																											
						$0.00 																											
						$0.00 																											
			('PNCBANK																														
											É																						
Investment Products  • Not FDIC Insured  • No Bank Guarantee  • May Lose Value" "($143,000,000.00) $210,000,000.00 $23,719,000,000,000.00 
$26,622,000,000,000.00 $26,465,000,000,000.00 $20,129,000,000,000.00 $2,774,000,000.00 $89,000,000.00 $2,992,000,000.00 $6,336,000,000.00 
$13,412,000,000.00 $157,000,000.00 $4,990,000,000.00 Q4 2020Q4 2019 Dec. 31, 2020Dec. 31, 2019 $182,527.00 $161,857.00 $84,732.00 $71,896.00 $27,573.00 
$26,018.00 $17,946.00 $18,464.00 $11,052.00 $9,551.00 $0.00 $1,697.00 $141,303.00 $127,626.00 $41,224.00 $34,231.00 $6,858,000,000.00 $5,394.00 
$22,677,000,000.00 $19,289,000,000.00 $22,677,000,000.00 $19,289,000,000.00 $22,677,000,000.00 $19,289,000,000.00 $20,210,418.00 RateUnitsTotalYTDTaxes / 
DeductionsCurrentYTD --$70,842,745,000.00 $70,842,745,000.00 Federal Withholding$0.00 $0.00 FICA - Social Security$0.00 $8,853.60 FICA - Medicare$0.00 
$0.00 Employer Taxes FUTA$0.00 $0.00 SUTA$0.00 $0.00 EIN: 61-1767919 SSN: 633441725 Gross $70,842,745,000.00 Earnings Statement Taxes / DeductionsStub 
Number: 1 $0.00 Net PaySSNPay SchedulePay PeriodSep 28, 2022 to Sep 29, 2023Pay Date$44,669.00 $70,842,745,000.00 XXX-XX-1725Annually CHECK NUMBER 
221116905560140 $76,033,000,000.00 $20,642,000,000.00 $18,936,000,000.00 $18,525,000,000.00 $17,930,000,000.00 $15,227,000,000.00 $11,247,000,000.00 
$6,959,000,000.00 $6,836,000,000.00 $10,671,000,000.00 $7,068,000,000.00 $76,033,000,000.00 $20,642,000,000.00 $18,936,000,000.00 $18,525,000,000.00 
$17,930,000,000.00 $15,227,000,000.00 $11,247,000,000.00 $6,959,000,000.00 $6,836,000,000.00 $10,671,000,000.00 $7,068,000,000.00 $76,033,000,000.00 
$20,642,000,000.00 $18,936,000,000.00 $18,525,000,000.00 $17,930,000,000.00 $15,227,000,000.00 $11,247,000,000.00 $6,959,000,000.00 $6,836,000,000.00 
$10,671,000,000.00 $7,068,000,000.00 $76,033,000,000.00 $20,642,000,000.00 $18,936,000,000.00 $257,637,000,000.00 $75,325,000,000.00 $65,118,000,000.00 
$61,880,000,000.00 $55,314,000,000.00 $56,898,000,000.00 $46,173,000,000.00 $38,297,000,000.00 $41,159,000,000.00 $46,075,000,000.00 $40,499,000,000.00 
$78,714,000,000.00 $21,885,000,000.00 $21,031,000,000.00 $19,361,000,000.00 $16,437,000,000.00 $15,651,000,000.00 $11,213,000,000.00 $6,383,000,000.00 
$7,977,000,000.00 $9,266,000,000.00 $9,177,000,000.00 $0.16 $0.18 $0.16 $0.16 $0.16 $0.16 $0.12 $0.18 $6,836,000,000.00 $7,977,000,000.00 $113.88 $31.15 
$28.44 $27.69 $26.63 $22.54 $16.55 $10.21 $9.96 $15.49 $10.20 $113.88 $31.12 $28.44 $27.69 $26.63 $22.46 $16.55 $10.21 $9.96 $15.47 $10.20 $112.20 $30.69 
$27.99 $27.26 $26.29 $22.30 $16.40 $10.13 $9.87 $15.35 $10.12 $112.20 $30.67 $27.99 $27.26 $26.29 $22.23 $16.40 $10.13 $9.87 $15.33 $10.12 $667,650,000.00 
$662,664,000.00 $665,758,000.00 $668,958,000.00 $673,220,000.00 $675,581,000.00 $679,449,000.00 $681,768,000.00 $686,465,000.00 $688,804,000.00 
$692,741,000.00 $677,674,000.00 $672,493,000.00 $676,519,000.00 $679,612,000.00 $682,071,000.00 $682,969,000.00 $685,851,000.00 $687,024,000.00 
$692,267,000.00 $695,193,000.00 $698,199,000.00 $9.87 $113.88 $31.15 $28.44 $27.69 $26.63 $22.54 $16.55 $10.21 $9.96 $15.49 $10.20 $1.00 $112.20 $30.69 
$27.99 $27.26 $26.29 $22.30 $16.40 $10.13 $9.87 $15.35 $10.12 $667,650,000.00 $662,664,000.00 $665,758,000.00 $668,958,000.00 $673,220,000.00 
$675,581,000.00 $679,449,000.00 $681,768,000.00 $686,465,000.00 $688,804,000.00 $692,741,000.00 $677,674,000.00 $672,493,000.00 $676,519,000.00 
$679,612,000.00 $682,071,000.00 $682,969,000.00 $685,851,000.00 $687,024,000.00 $692,267,000.00 $695,193,000.00 $698,199,000.00 Amount Transaction 
descriptionFor the period 04/13/2022 to 04/29/2022 ZACHRY TYLER WOOD Primary account number: 47-2041-6547 Page 2 of 3"" $22,116,905,560,149.00 Reference 
number Amount Transaction description $22,116,905,560,149.00 $62.50 Corporate ACH Quickbooks 180041ntuit 1940868 Reference number $22,116,905,560,149.00 
Amount Transaction description on your next statement as a single line item entitled Service Waived - New Customer Period $36.00 Returned Item Fee (nsf)



Amount																			


$0.00 																			

				#NAME?																													
							step 2: Add together checks and other deductions listed in your account register but not on your statement.																										
								C'eck Deduction Descretio•	Anount																								
		account or benefit, or in payment of the individual obligations of, any individual obligations of any such persons to the Bank without regard to the disposition or purpose of same as allowed by applicable law.																															

$0.00 																											

						Taxpayer I.D. Number (TIN)																											
				(Customer"")"		633-44-1725																										
Revolving Credits. Resolved that in connection with any extension of credit obtained by any of the persons authorized in Section 5 above, that permit the Customer to effect multiple advances or draws under such credit, any of the persons listed in Sections 5 (Loans and Extensions of Credit) and 3 (Withdrawals and Endorsements) Resolution for ALPHABET Telephonic and Facsimile Requests. Resolved that the Bank is authorized to take any action authorized hereunder based upon (i) the telephone request of any person purporting to be a person authorized to act hereunder, (ii) the signature of any person authorized to act hereunder that is delivered to the Bank by facsimile transmission, or (iii) the telex originated by any of such persons, tested in accordance with such testing : Tr R •d Ming procedures as may be established between the Customer and the Bank from time to time. General. Resolved that a certified copy of these resolutions be delivered to the Bank; that the persons specified herein are vested with authority to act and may designate successor persons to act on behalf of Customer without further authority from the Customer or governing body; and that Bank may rely on the authority given by this resolution until actual receipt by the Bank of a certified copy of a new resolution modifying or revoking the Customer Copy, page 2 of 4 Withdrawals and Transfers. Resolved that the Bank is authorized to make payments from the account(s) of Customer according to any check, draft, bill of exchange, acceptance or other written instrument or direction signed by any one of the following individuals, officers or designated agents, and that such designated individuals may also otherwise transfer, or enter into agreements with Bank concerning the transfer, of funds from Customer's account(s), whether by telephone, telegraph, computer or any other manner:
('PNCBANK																		

		45999-0023																															
									44658																								

						SS-4																											
										CP 575 A																							
É																			


		75235																															

								We assigned you																									
	This EIN will identify you, your business accounts, tax returns, and																																

							If the information is																										
#NAME?																			


step 2: Add together checks and other deductions listed in your account register but not on your statement.						


C'eck Deduction Descretio•	Anount															


account or benefit, or in payment of the individual obligations of, any individual obligations of any such persons to the Bank without regard to the 
disposition or purpose of same as allowed by applicable law.												

										Please				b																			




									6.35-																								
														8																			
					Total Year to Date										3,																		
			Total for this Period																														
			36		36																												
															18																		
															t ly of																		
		Items	Amount		Checks and Other Deductions Description						Items	Amount																					
		1	62.5		ACH Deductions						1	62.5			he																		
					Service Charges and Fees						1	36																					
		1	62.5		Total						2	98.5																					
			Date		Ledger balance			Date				Ledger balance																					

			(279		62.50-			44678				36																					
Ledger balance	*		You'																														
			202																														
			otm corr																														
			esti																														
TM 27.8414.76%		63500	53.:																														
			202																														
		2160	gro																														
		550	ovr																														
Taxpayer I.D. Number (TIN)																


(Customer"")"		633-44-1725															


Revolving Credits. Resolved that in connection with any extension of credit obtained by any of the persons authorized in Section 5 above, that permit the 
Customer to effect multiple advances or draws under such credit, any of the persons listed in Sections 5 (Loans and Extensions of Credit) and 3 
(Withdrawals and Endorsements) Resolution for ALPHABET Telephonic and Facsimile Requests. Resolved that the Bank is authorized to take any action 
authorized hereunder based upon (i) the telephone request of any person purporting to be a person authorized to act hereunder, (ii) the signature of any 
person authorized to act hereunder that is delivered to the Bank by facsimile transmission, or (iii) the telex originated by any of such persons, tested 
in accordance with such testing : Tr R •d Ming procedures as may be established between the Customer and the Bank from time to time. General. Resolved 
that a certified copy of these resolutions be delivered to the Bank; that the persons specified herein are vested with authority to act and may designate 
successor persons to act on behalf of Customer without further authority from the Customer or governing body; and that Bank may rely on the authority 
given by this resolution until actual receipt by the Bank of a certified copy of a new resolution modifying or revoking the Customer Copy, page 2 of 4 
Withdrawals and Transfers. Resolved that the Bank is authorized to make payments from the account(s) of Customer according to any check, draft, bill of 
exchange, acceptance or other written instrument or direction signed by any one of the following individuals, officers or designated agents, and that such 
designated individuals may also otherwise transfer, or enter into agreements with Bank concerning the transfer, of funds from Customer's account(s), 
whether by telephone, telegraph, computer or any other manner:



45999-0023																		


44658																			




SS-4																			


CP 575 A																		






75235																			




We assigned you																		


This EIN will identify you, your business accounts, tax returns, and											




If the information is																	

















Please				b															










6.35-																			


8																			

Total Year to Date										3,							


Total for this Period																	


36		36																	


18																		

t ly of																		

Items	Amount		Checks and Other Deductions Description						Items	Amount					


1	62.5		ACH Deductions						1	62.5			he					


Service Charges and Fees						1	36									


1	62.5		Total						2	98.5									


Date		Ledger balance			Date				Ledger balance								



(279		62.50-			44678				36										


Ledger balance	*		You'															


202																			


otm corr																		


esti																			


TM 27.8414.76%		63500	53.:															



202																														

2160	gro																		


550	ovr																		





@@ -178,182 +498,124 @@ TM 27.8414.76%		63500	53.:



	525000																																
$0.001 0.00%	367500																																

	$708,750.00 Medium Wide																																
	Standard																																





ING  payments, or replying to any related correspondence,																																	


			If the information is																														
							We assigned you																										
	44658																																
	44658																																
	44658				If the information is																												
	44658																																
	44658																																

				Plea S																													
		If there is a balance due on the return (s)																															

If you were not in business or did not hire any employees																																	
								PI																									
				If you																													


						#NAME?																											
						Primary account number: 47-2041-6547 Page 1 of 3																											
				1022462	Q 304	Number of enclosures: 0																											
						For 24-hour banking sign on to PNC Bank Online Banking on pnc.com FREE Online Bill Pay For customer service call 1-877-BUS-BNKG PNC accepts Telecommunications Relay Service (TRS) calls.				9																							
						Para servicio en espalol, 1877.BUS-BNKC, Moving? Please contact your local branch. @ Write to: Customer Service PO Box 609 Pittsburgh , PA 15230-9738 Visit us at PNC.com/smaIIbusiness																											
										Date of this notice:																							
															44658																		
							Zachry Tyler Wood Alphabet			Employer Identification Number: 88-1656496																							
					Checks and other deductions		Ending balance			Form:		SS-4																					
			Deposits and other additions							Number of this notice:						CP 575 A																	
			#ERROR!		98.50 Average ledger balance		36.00- Average collected balance			For assistance you may call ug at:																							
					6.35-			6.35-		1-800-829-4933																							
					Total Year to Date																												
		Total for this Period																															
		36			36					IF YOU WRITE, ATTATCHA TYE STUB AT OYE END OF THIS NOTICE.																							
	Items	Amount			Checks and Other Deductions Description			Items	Amount																								
	1	62.5			ACH Deductions			1	62.5					We assigned you																			
					Service Charges and Fees			1	36																								
	1	62.5			Total			2	98.5																								
		Date				Date			Ledger balance				If the information is																				
Ledger balance					Ledger balance																												
0		44677			62.50-		44678		36																								
	Form 940						44658																										
Berkshire Hathaway																																	
					For the period 04/13/2022  to 04/29/2022		44680																										
					ZACHRY TYLER WOOD																												
					Primary account number: 47-2041-6547 Page 2 of 3											Please																	
					Page 2 of 3																												

											did not hire any employee																						
								Referenc numb																									
$ Current			Shares			$ Market	%																										
Price	$ Change		% Change Held			Value	Weight																										
2749.75		21.24	0.78		500000000	1374875000000	5.65																										
2749.75			0.78	9,000,000,000.00 24,747,750,000,000.00			101.67																										
1		0	0	65000000		65000000	0																										
1		0	0	1		1	0																										
1		0	0	-9000000		-9000000	0																										
1		0	0	127537500000		127537500000	0.52																										
1		0	0 -1909,957,000,000.00			-1909957000000	-7.85																										
	201780000000		0.84			24340261500001	99.99																										
	T Lc•s Detail .. May u 2022																																

														• OCE																			
					GOOG		Morningstar Rating																										

																										Ascending							
																				Export													
							41609		41974		42339															2021-12							
					41244																												
																						44166											
														42705		43070		43435		43800													
					50175		59825		66001		74989															$257,637.00			$257,637.00				
														90272		110855		136819		161857		182527											
					58.9		56.8		61.1		62.4															$56.90			$56.90				
														61.1		58.9		56.5		55.6		53.6											
					12760		13966		16496		19360			23716		28882		31392		35928		41224				$78,714.00			$78,714.00				
																										$30.60			$30.60				
					25.4		23.3		25		25.8			26.3		26.1		22.9		22.2		22.6											
					10737		12920		14444		16348			19478		12662		30736		34343		40269				$76,033.00			$76,033.00				
					16.16		18.79		20.57		22.84			27.85		18		43.7		49.16		58.61				$112.20			$112.20				
If you were not in business or did not hire any employees												



					665		678		687		693			699		704		703		699			687				$678.00		$678.00				
									145.08		169.12			193.99		226.11		244.18		283.25			315.33				$369.37		$380.70				
					16619		18659		22376		26024			36036		37091		47971		54520			65124				$91,652.00		$91,652.00				
					-3273		-7358		-10959		-9915			-10212		-13184		-25139		-23548			-22281				-$24,640.00			-$24,640.00			
		Mil			13346		11301		11417		16109			25824		23907		22832		30972			42843				$67,012.00			$67,012.00			
									16.92		21.15			33.65		34.57		32.49		40.72			49.3				$96.52						
					46117		56978		63880		70804			88652		100125		101056		107357			117462				$123,889.00						
PI																			


		Growth	Cash Flow			Financial Health		Efficiency Ratios																									
				2012-12 2013-12						41974		42339			42705		43070		43435		43800			44166				2021-12				TTY	
				100.00  100.00						100		100			100		100		100		100			100				$100.00				$100.00	
				41.12   43.22						38.93		37.56			38.92		41.12		43.52		44.42			46.42				$43.06				$43.06	
				58.88   56.78						61.07		62.44			61.08		58.88		56.48		55.58			53.58				$56.94				$56.94	
				19.91   20.14						21.18		20.25			19.35		17.83		17.88		17.31			15.89				$14.14				$14.14	
				13.54   13.29						14.9		16.38			15.45		15		15.65		16.07			15.11				$12.25				$12.25	
				25.43   23.34						24.99		25.82			26.27		26.05		22.94		22.2			22.59				$30.55				$30.55	
				1.25    0.89						1.16		0.39			0.48		-1.52		2.57		2.28			3.76				$4.67				$4.67	
				26.68   24.23						26.15		26.21			26.75		24.53		25.52		24.48			26.34				$35.22				$35.22	
				2012-12 2013-12						41974		42339			42705		43070		43435		43800			44166				2021-12				TTM	
				19.41   15.74						19.3		16.81			19.35		53.44		11.96		13.33			16.25				$16.20				$16.20	
				21.40   21.60						21.88		21.1			21.58		11.42		22.46		21.22				$22.06				$29.51			$29.51	
				0.60    0.58						0.55		0.54			0.57		0.61		0.64			0.64			$0.61				$0.76				$0.76
				12.91   12.62						11.93		11.36			12.37		6.94		14.29			13.5			$13.52				$22.40				$22.40
				1.31    1.27						1.25		1.23			1.2		1.29			1.31		1.37			$1.44				$1.43				$1.43
				16.54   16.25						15.06		14.08			15.02		8.69			18.62		18.12			$19.00				$32.07				32,07
				14.66   14.52						13.77		12.82			14.02			7.98		17.26		16.15				$16.63			$28.36				$28.36
				160.36  175.65						171.88		189.95			195.76			250.48		307.25		397.25				$357.16			$263.24				$263.24
				 ' '								wo			oo•ooo'ooo'08L'voz											POOM 101K' Ousn							
						ALPHABÉT L.L.C. Profit and Loss Detail January 1 - May 22, 2022																											
TRANSACTION NUM TYPE					NAME		MEMO/DESCRIPTION SPLIT		AMOUNT	BALANCE																							
If you																			

				1004																													
	Sales Receipt																																
					Internal Revenue Service (IRS) - Internal Revenue Service 1111 Constitution Ave. N.W. Washinton DC 20535		Transaction description Effective 04-27-22 Amount 36.00 Reverse ACH Debit	Business Checking	0	0																							
							Effective 04-26-22																										
							Reference number																										
							22116905560140																										
	Sales Receipt			1003	Internal Revenue Service (IRS) - Internal Revenue Service 1111 Constitution Ave. N.W. Washinton DC 20535		Transaction description Effective 04-26-22 Amount 62.50 Reverse ACH Debit	Business Checking	70,842,743,866.00 70,842,743,866.00																								
							Effective 04-26-22 Reference number																										
							22116905560149																										
									70842743866																								
									70842743866																								
ZACHRY TYLER WOOD :																			
Primary account number: 47-2041-6547 Page 1 of 3													

				00022116905560149 All figures are estimates based on samples—money amounts are in ZAC thousands of dollars - INCOME				Accounts Payable (XP)																									
Number of enclosures: '[12753750'.'[00']m']'	:												

							bitcoin'	Accounts																									
		Bill		7364071921891																													
								Payable (AP)	#ERROR!																								
For 24-hour banking sign on to PNC Bank Online Banking on pnc.com FREE Online Bill Pay For customer service call 1-877-BUS-BNKG PNC accepts 
Telecommunications Relay Service (TRS) calls.				9										

			SVCCHRG																														
								Business Checking	62.5	62.5																							
			fees																														
			ard fees						62.5																								
Para servicio en espalol, 1877.BUS-BNKC, Moving? Please contact your local branch. @ Write to: Customer Service PO Box 609 Pittsburgh , PA 15230-9738 
Visit us at PNC.com/smaIIbusiness															

									12753754062																								
									1275379063																								
									$58,0849egeoa50																								
Date of this notice:																	

44658																		

Zachry Tyler Wood Alphabet			Employer Identification Number: 88-1656496								

Checks and other deductions

Ending balance			Form:		SS-4																					
Deposits and other additions							Number of this notice:						CP 575 A

98.50 Average ledger balance		36.00- Average collected balance			For assistance you may call ug at:	

6.35-			6.35-		1-800-829-4933													

Total Year to Date																	
\
Total for this Period																	
36			
36					
IF YOU WRITE, ATTATCHA TYE STUB AT OYE END OF THIS NOTICE.																							

Items	Amount			Checks and Other Deductions Description			Items	Amount							


1	62.5			ACH Deductions			1	62.5					We assigned you				


Service Charges and Fees			1	36												


1	62.5			Total			2	98.5											


Date				Date			Ledger balance				If the information is																				


Ledger balance					Ledger balance																												

0		44677			62.50-		44678		36										


Form 940						44658												


Berkshire Hathaway																	


For the period 04/13/2022  to 04/29/2022		44680												

					ZACHRY TYLER WOOD												


					Primary account number: 47-2041-6547 Page 2 of 3											Please		


					Page 2 of 3																		



					did not hire any employee																


					Referenc numb																		

$ Current			Shares	
$ Market	%																	


Price	$ Change		% Change Held			Value	Weight										

2749.75		21.24	0.78		500000000	1374875000000	5.65										


2749.75			0.78	9,000,000,000.00 24,747,750,000,000.00			101.67								


1		0	0	65000000		65000000	0										


1		0	0	1		1	0												


1		0	0	-9000000		-9000000	0										


1		0	0	127537500000		127537500000	0.52																										



@@ -362,47 +624,141 @@ TRANSACTION NUM TYPE					NAME		MEMO/DESCRIPTION SPLIT		AMOUNT	BALANCE


Transaction Amount description					Reference																												
					number																												
70842743866		Corporate ACH Quickbooks 180041ntuit 1940868																															
					|00022116905560149|																												
					number														

70842743866		Corporate ACH Qu
ickbooks 180041ntuit 1940868																


|00022116905560149|																	




Transaction Amount descripton					Reference										


0					number														


|00022116905560149|																	






Amount																			


62.5																			


36																													

98.5	Waived -	Waived - New Customer Period													




PNCBANK																			


Volume																			

The activity detail section of your statement to your account register.											


All items in your account register that also appear on your statement. Remember to begin with the ending date of your last statement. (An asterisk { * } 
will appear in the Checks section if there is a gap in the listing of consecutive check numbers.) Any deposits or additions including interest payments 
and ATM or electronic deposits listed on the statement that are not already entered in your register. Any account deductions including fees and ATM or 
electronic deductions listed on the statement that are not already entered in your register.								


step 2: Add together checks and other deductions listed in your account register but not on your statement.						


Amount						Check Deduction Descrption	Amount									




on Deposit:																		













Total A=$22934637118600																	




22934637118600																		







Total A + $22934637118600																




Subtotal=$22934637118600																


$	22934637118600																	




$	22934637118600																	


Total B22934637118600																	















Transaction Amount descripton					Reference																												
0					number																												
					|00022116905560149|																												


				Amount																													
				62.5																													
				36																													
				98.5	Waived -	Waived - New Customer Period																											

										PNCBANK																							
				Volume																													
The activity detail section of your statement to your account register.																																	
	All items in your account register that also appear on your statement. Remember to begin with the ending date of your last statement. (An asterisk { * } will appear in the Checks section if there is a gap in the listing of consecutive check numbers.) Any deposits or additions including interest payments and ATM or electronic deposits listed on the statement that are not already entered in your register. Any account deductions including fees and ATM or electronic deductions listed on the statement that are not already entered in your register.																																
								step 2: Add together checks and other deductions listed in your account register but not on your statement.																									
			Amount						Check Deduction Descrption	Amount																							

on Deposit:																																	



0																			



Total A=$22934637118600																																	

					22934637118600																												



				Total A + $22934637118600																													

				Subtotal=$22934637118600																													
						$	22934637118600																										

						$	22934637118600																										
									Total B22934637118600																								



@@ -413,7 +769,6 @@ Total A=$22934637118600



			0																														



0 comments on commit 96c1a80
@zakwarlord7
 
Add heading textAdd bold text, <Ctrl+b>Add italic text, <Ctrl+i>
Add a quote, <Ctrl+Shift+.>Add code, <Ctrl+e>Add a link, <Ctrl+k>
Add a bulleted list, <Ctrl+Shift+8>Add a numbered list, <Ctrl+Shift+7>Add a task list, <Ctrl+Shift+l>
Directly mention a user or team
Reference an issue, pull request, or discussion
Add saved reply
Leave a comment
No file chosen
Attach files by dragging & dropping, selecting or pasting them.
Styling with Markdown is supported
 You’re receiving notifications because you’re watching this repository.
Footer
© 2022 GitHub, Inc.
Footer navigation
Terms
Privacy
Security
Status
Docs
Contact GitHub
Pricing
API
Training
Blog
About
Update README.md · zakwarlord7/Skip-to-content-Search-or-jump-to-Pull-requests-Issues-Codespaces-Marketplace-Explore-zakwarlord@96c1a80
