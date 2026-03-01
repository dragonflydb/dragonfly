1. To move lua_project to dragonfly from helio (DONE)
2. To limit lua stack to something reasonable like 4096.
3. To inject our own allocator to lua to track its memory.


## Object lifecycle and thread-safety.

Currently our transactional and locking model is based on an assumption that any READ or WRITE
access to objects must be performed in a shard where they belong.

However, this assumption can be relaxed to get significant gains for read-only queries.

### Explanation
Our transactional framework prevents from READ-locked objects to be mutated. It does not prevent from their PrimaryTable to grow or change, of course. These objects can move to different entries inside the table. However, our CompactObject maintains the following property - its reference CompactObject.AsRef() is valid no matter where the master object moves and it's valid and safe for reading even from other threads. The exception regarding thread safety is SmallString which uses translation table for its pointers.

If we change the SmallString translation table to be global and thread-safe (it should not have lots of write contention anyway) we may access primetable keys and values from another thread and write them directly to sockets.

Use-case: large strings that need to be copied. Sets that need to be serialized for SMEMBERS/HGETALL commands etc. Additional complexity - we will need to lock those variables even for single hop transactions and unlock them afterwards. The unlocking hop does not need to increase user-visible latency since it can be done after we send reply to the socket.\- 5/4/2022 - 6/4/2022                                        00519                                                                         
+88-1303491        
State ID: 00037305581
        SSN: 633-44-1725
        00000                                                                                         
Employee Number: 3 
Description        Amount                                                        
5/4/2022 - 6/4/2022                                                 
Payment Amount (Total)        9246754678763                                                        Display All                                              
1. Social Security (Employee + Employer)                        26662                                                                                       
2. Medicare (Employee + Employer)                        861193422444                                        Hourly                                              
3. Federal Income Tax Return Over-payment 1040-ES Refundded Refund Refunding                        8385561229657                                        00000                                                 
Note: This report is generated based on the payroll data for your reference only. Please contact IRS office for special cases such as late payment, previous overpayment, penalty and others.  
+Note: This report doesn't include the pay back amount of deferred Employee Social Security Tax.                                                                                                                 
Employer Customized Report + +# +ADP + +Report Range5/4/2022 - 6/4/2022        88-1656496        state ID: 633441725        Ssn :XXXXX1725        State:         All        Local ID: 00037305581               
226770000000000                                         
EIN:                                                                                                                 
Customized Report                Amount                                             Employee Payment Report                                                                            ADP                                                
Employee Number: 3 
transaction description                                                                                                              +
Wages, Tips and Other Compensation                22662983361014                                               
 Tips                                                 
Taxable SS Wages                215014                                                5105000                                              
Taxable SS Tips                00000                                                                                                 
Taxable Medicare Wages                22662983361014                
Salary               
 Vacation hourly              
  OT                                                
Advanced EIC Payment                00000                3361014                                                                           
Federal Income Tax Withheld                8385561229657                
Bonus                00000                00000                                                
Employee SS Tax Withheld                13331                00000                Other Wages 1                Other Wages 2                                                 
Employee Medicare Tax Withheld                532580113436                
Total                00000                00000                                                
State Income Tax Withheld                00000                
22662983361014                                                                             
Local Income Tax Withheld + +Customized Employer Tax Report                00000                Deduction Summary                                                                              
Description                Amount                Health Insurance                                                                                 
Employer SS Tax + +Employer Medicare Tax                13331                00000                                                                                
 +Federal Unemployment Tax                328613309009                Tax Summary                                                                               
 + +State Unemployment Tax                00442                
Federal Tax        00007                        Total Tax                                                 
Customized Deduction Report                00840                $8,385,561,229,657@3,330.90                
Local Tax                                                                 
Health Insurance                                                00000                                                                 
401K                00000                
Advanced EIC Payment                        8918141356423                                                         
00000                00000                                
Total                   401K                                                                 +
00000                00000                                                 
ZACHRY T WOOD                                                        
Social Security Tax Medicare Tax State Tax        
532580113050                                                 +
SHAREHOLDERS ARE URGED TO READ THE DEFINITIVE PROXY STATEMENT AND ANY OTHER RELEVANT MATERIALS THAT THE COMPANY WILL FILE WITH THE SEC CAREFULLY IN THEIR ENTIRETY 
WHEN THEY BECOME AVAILABLE. SUCH DOCUMENTS WILL CONTAIN IMPORTANT INFORMATION ABOUT THE COMPANY AND ITS DIRECTORS, OFFICERS  AND 
AFFILIATES. INFORMATION REGARDING THE INTERESTS OF CERTAIN OF THE COMPANY’S DIRECTORS, OFFICERS AND AFFILIATES WILL BE AVAILABLE IN THE 
DEFINITIVE PROXY STATEMENT.                
This Definitive Proxy Statement and any other relevant materials that will be filed with the SEC will be available free of charge at the SEC’s website at www.sec.gov
 In addition, the Definitive Proxy Statement (when available) and other relevant documents will also be available, without + +charge, by directing a request by mail to Attn: Investor Relations, 
Alphabet Inc., 1600 Amphitheatre Parkway, Mountain View, California, 94043 or by contacting investor-relations@abc.xyz. The Definitive Proxy Statement and other relevant documents will also be available on the Company’s 
Investor Relations website at https://abc.xyz/investor/other/annual-meeting/.                                                                                                                 +                                                                                                                 
The Company and its directors and certain of its executive officers may be consideredno participants in the solicitation of proxies with respect to the proposals under the Definitive Proxy Statement under the rules of the SEC. 
Additional information regarding the participants in the proxy  solicitations and a description of their direct and indirect interests, by security holdings or otherwise, also will be included in the Definitive Proxy Statement
 and other relevant materials to be filed with the SEC when they become available.                                .        9246754678763                                                                         
3/6/2022 at 6:37 PM                                                                                                             
Q4 2021                   Q3 2021                Q2 2021                Q1 2021               Q4 2020                                                 +                                                                                                                 
GOOGL_income-statement_Quarterly_As_Originally_Reported                                 
24934000000        25539000000        37497000000        31211000000        30818000000

                                                 +                                 +
24934000000        25539000000        21890000000        19289000000        22677000000                                                 + +
Cash Flow from Operating Activities, Indirect                                
24934000000        25539000000        21890000000        19289000000        22677000000                                                 + +
Net Cash Flow from Continuing Operating Activities, Indirect                               
 20642000000        18936000000        18525000000        17930000000        15227000000                                                 + +
Cash Generated from Operating Activities                               
+ 6517000000        3797000000        4236000000        2592000000        5748000000                                                 + 
+Income/Loss before Non-Cash Adjustment                                
+3439000000        3304000000        2945000000        2753000000        3725000000                                                 + +
+Total Adjustments for Non-Cash Items                               
+ 3439000000        3304000000        2945000000        2753000000        3725000000                                                 + +
+Depreciation, Amortization and Depletion, Non-Cash Adjustment                                
+3215000000        3085000000        2730000000        2525000000        3539000000                                                 + +
+Depreciation and Amortization, Non-Cash Adjustment                               
+ 224000000        219000000        215000000        228000000        186000000                                                 + +
+Depreciation, Non-Cash Adjustment                               
+ 3954000000        3874000000        3803000000        3745000000        3223000000                                                 + +
+Amortization, Non-Cash Adjustment                                1
+616000000        -1287000000        379000000        1100000000        1670000000                                                 + +
+Stock-Based Compensation, Non-Cash Adjustment                                -
+2478000000        -2158000000        -2883000000        -4751000000        -3262000000                                                 + +
+Taxes, Non-Cash Adjustment                               
+ -2478000000        -2158000000        -2883000000        -4751000000        -3262000000                                                 + 
++Investment Income/Loss, Non-Cash Adjustment                                
+-14000000        64000000        -8000000        -255000000        392000000               
+                                
+ Gain/Loss on Financial + +Instruments, Non-Cash Adjustment                                
+-2225000000        2806000000        -871000000        -1233000000        1702000000                                                 + +
+Other Non-Cash Items                                -
+5819000000        -2409000000        -3661000000        2794000000        -5445000000                                                 + +
+Changes in Operating Capital       
+ -5819000000        -2409000000        -3661000000        2794000000        -5445000000                                                 + +
+Change in Trade and Other Receivables                                
+-399000000        -1255000000        -199000000        7000000        -738000000                                                 + +
+Change in Trade/Accounts Receivable                               
+ 6994000000        3157000000        4074000000        -4956000000        6938000000                                         + +
+Change in Other Current Assets                                
+1157000000        238000000        -130000000        -982000000        963000000                                                 + +
+Change in Payables and Accrued Expenses                                
+1157000000        238000000        -130000000        -982000000        963000000                                                 + +
+Change in Trade and Other Payables                                
+5837000000        2919000000        4204000000        -3974000000        5975000000                                                 + +
+Change in Trade/Accounts Payable                               
+ 368000000        272000000        -3000000        137000000        207000000                                                 + +
+Change in Accrued Expenses                                
+-3369000000        3041000000        -1082000000        785000000        740000000                                                 + +
+Change in Deferred Assets/Liabilities                                                                                                                 + +
+Change in Other Operating Capital                                                                                                                 + +-
+11016000000        -10050000000        -9074000000        -5383000000        -7281000000                                                 + +
+Change in Prepayments and Deposits                                
+-11016000000        -10050000000        -9074000000        -5383000000        -7281000000                                                 + +
+Cash Flow from Investing Activities                                                                                                                 + +
+Cash Flow from Continuing Investing Activities                               
+ -6383000000        -6819000000        -5496000000        -5942000000        -5479000000                                                 + +
+-6383000000        -6819000000        -5496000000        -5942000000        -5479000000                                                 + +
+Purchase/Sale and Disposal of Property, Plant and Equipment, Net                                                                                                                 + 
++Purchase of Property, Plant and Equipment                                
+-385000000        -259000000        -308000000        -1666000000        -370000000                                                 + +
+Sale and Disposal of Property, Plant and Equipment                                
+-385000000        -259000000        -308000000        -1666000000        -370000000                                                 + +
+Purchase/Sale of Business, Net                                
+-4348000000        -3360000000        -3293000000        2195000000        -1375000000                                                 + +
+Purchase/Acquisition of Business                                
+-40860000000        -35153000000        -24949000000        -37072000000        -36955000000                                                 + +
+Purchase/Sale of Investments, Net                                                                                                                 + 
++Purchase of Investments                                
+36512000000        31793000000        21656000000        39267000000        35580000000                                                 + 
++100000000        388000000        23000000        30000000        -57000000                                                 + +
+Sale of Investments                                                                                                                 + +
+Other Investing Cash Flow                                        -15254000000                                                                         + +
+Purchase/Sale of Other Non-Current Assets, Net                               
+ -16511000000        -15254000000        -15991000000        -13606000000        -9270000000                                                 + +
+Sales of Other Non-Current Assets                                
+-16511000000        -12610000000        -15991000000        -13606000000        -9270000000                                                 + +
+Cash Flow from Financing Activities                                ]
+-13473000000        -12610000000        -12796000000        -11395000000        -7904000000                                                 + +
+Cash Flow from Continuing Financing Activities                                
+13473000000                -12796000000        -11395000000        -7904000000                                                 + +
+Issuance of/Payments for Common 343 sec cvxvxvcclpddf wears
+Stock, Net                                       
+ -42000000                                                                         + +
+Payments for Common Stock                                
+115000000        -42000000        -1042000000        -37000000        -57000000                                                 + +
+Proceeds from Issuance of Common Stock                               
+ 115000000        6350000000        -1042000000        -37000000        -57000000                                                 + +
+Issuance of/Repayments for Debt, Net                                
+6250000000        -6392000000        6699000000        900000000        00000                                                 + +
+Issuance of/Repayments for Long Term Debt, Net                                
+6365000000        -2602000000        -7741000000        -937000000        -57000000                                                 + +
+Proceeds from Issuance of Long Term Debt                                                                                                                 + +
+Repayments for Long Term Debt                                
+2923000000                -2453000000        -2184000000        -1647000000                                                 + +
+Proceeds from Issuance/Exercising of Stock Options/Warrants                                
+00000                300000000        10000000        338000000000                                                 + +
+Other Financing Cash Flow                                                                                                                 + +
+Cash and Cash Equivalents, End of Period                                                                                                                 + +
+Change in Cash                                
+20945000000        23719000000        23630000000        26622000000        26465000000                                                 + +
+Effect of Exchange Rate Changes                                
+25930000000        235000000000        -3175000000        300000000        6126000000                                                 + +
+Cash and Cash Equivalents, Beginning of Period                                
+PAGE="$USD(181000000000)".XLS        
+BRIN="$USD(146000000000)".XLS        
+183000000        -143000000        210000000 + +
+Cash Flow Supplemental Section                                
+23719000000000                26622000000000        
+26465000000000        20129000000000                                                 + +
+Change in Cash as Reported, Supplemental                                
+2774000000        89000000        -2992000000                6336000000                                                 + +
+Income Tax Paid, Supplemental                                
+13412000000        157000000                                                                         + +
+ZACHRY T WOOD                                                                \
+-4990000000                                                 + +
+Cash and Cash Equivalents, Beginning of Period                                                                                                                 + +
+Department of the Treasury                                                                                                                 + +
+Internal Revenue Service                                                                                                                 + + +
+Q4 2020                        Q4 2019                                                 + +
+Calendar Year                                                                                                                 + +
+Due: 04/18/2022                                                                                                                 + +
+Dec. 31, 2020                        Dec. 31, 2019                                                 + +
+USD in "000'"s                                                                                                                 + +
+Repayments for Long Term Debt                                       
+182527                        161857                                                 + +
+Costs and expenses:                                                                                                                 + +
+Cost of revenues                                        
+84732                        71896                                                 + +
+Research and development                                        
+27573                        26018                                                 + +
+Sales and marketing                                        
+17946                        18464                                                 + +
+General and administrative                                       
+ 11052                        09551                                                 + +
+European Commission fines                                       
+ 00000                        01697                                                 + +
+Total costs and expenses                                        
+141303                        127626                                                 + +I
+ncome from operations                                       
+41224                        34231                                                 + +
+Other income (expense), net                                        
+6858000000                        05394                                                 + +
+Income before income taxes                                        22677000000                        19289000000                                                 + +
+Provision for income taxes                                           22677000000                        19289000000                                                 + + +
+Net income                                                                   22677000000                        19289000000                                                 + +
+
+*include interest paid, capital obligation, and underweighting                                                                                                                 + + + +
+
+Basic net income per share of Class A and B common stock and Class C capital stock (in dollars par share)    
+
+
+Diluted net income per share of Class A and Class B common stock and Class C capital stock (in dollars par share)                                                                                                                 +
+
+*include interest paid, capital obligation, and underweighting                                                                                                                 +                                                                                                                 +
+
+Basic net income per share of Class A and B common stock and Class C capital stock (in dollars par share)                                                                                                                 +
+
+Diluted net income per share of Class A and Class B common stock and Class C capital stock (in dollars par share)                                                                                                        '*''*'
+
+20210418                                                                                                 +
+Rate        Units        Total        YTD        Taxes / Deductions        Current        YTD        
+70842745000        70842745000        Federal Withholding        00000        188813800                                         +
+FICA - Social Security        00000        853700                                         +
+FICA - Medicare        00000        11816700                                         +
+Employer Taxes                                         +
+FUTA        00000        00000                                         +
+SUTA        00000        00000                                         +
+EIN: 61-1767919         +ID : 00037305581         +SSN: 633441725                                 +
+ATAA Payments        00000        102600         +
+Gross                                                                                                 +70842745000         +
+Earnings Statement                                                                                         +
+Taxes / Deductions         +
+Stub Number: 1                                                                                         +
+00000                                                                                                 +
+Net Pay         +
+SSN         +
+Pay Schedule         +
+Pay Period        : +
+Sep 28, 2022 : to : Sep 29, 2023        : 
+Pay Date : 4/18/2022 :                                                 +
+70842745000        : +
+XXX-XX-1725        : +
+Quarterly :                                                                         +
+CHECK NO. :                                                                                                 +
+22229                                                                                                 +
+INTERNAL REVENUE SERVICE,                                                                                                                 +
+PO BOX 1214,                                                                                                                 +
+CHARLOTTE, NC 28201-1214                                                                                                                                                                                         +
+# ZACHRY WOOD                                                                                                                 +
+# 00015                
+76033000000        20642000000        18936000000        18525000000        17930000000        15227000000        11247000000        6959000000        6836000000        10671000000        7068000000                 +
+
+For Disclosure, Privacy Act, and Paperwork Reduction Act Notice, see instructions +
+instructions :
+76033000000        20642000000        18936000000        18525000000        17930000000        15227000000        11247000000        6959000000        6836000000        10671000000        7068000000                 
++# Cat. No. 11320B                
+76033000000        20642000000        18936000000        18525000000        17930000000        15227000000        11247000000        6959000000        6836000000        10671000000        7068000000                 
++# Form 1040 (2021)                
+76033000000        20642000000        18936000000                                                                                 
++# Reported Normalized and Operating Income/Expense Supplemental Section                                                                                                                 
++# Total Revenue as Reported, Supplemental                
+257637000000        75325000000        65118000000        61880000000        55314000000        56898000000        46173000000        38297000000        41159000000        46075000000        40499000000                 
++# Total Operating Profit/Loss as Reported, Supplemental                
+78714000000        21885000000        21031000000        19361000000        16437000000        15651000000        11213000000         +# 6383000000        7977000000        9266000000        9177000000                 
++# Reported Effective Tax Rate                
+00000        00000        00000        00000        00000                00000        00000        00000                00000                 
++# Reported Normalized Income                                                                                
+6836000000                                 
++# Reported Normalized Operating Profit                                                                               
+ 7977000000                                 
++# Other Adjustments to Net Income Available to Common Stockholders                                                                                                                 
++# Discontinued Operations                                                                                                                 
++# Basic EPS               
+ 00114        00031        00028        00028        00027        00023        00017        00010        00010        00015        00010                 
++# Basic EPS from Continuing Operations                
+00114        00031        00028        00028        00027        00022        00017        00010        00010        00015        00010                 
++# Basic EPS from Discontinued Operations                                                                                                                 
++# Diluted EPS                
+00112        00031        00028        00027        00026        00022        00016        00010        00010        00015        00010                 
++# Diluted EPS from Continuing Operations               
+ 00112        00031        00028        00027        00026        00022        00016        00010        00010        00015        00010                 
++# Diluted EPS from Discontinued Operations                                                                                                                 
++# Basic Weighted Average Shares Outstanding                
+667650000        662664000        665758000        668958000        673220000        675581000        679449000        681768000        686465000        688804000        692741000                 
Diluted Weighted Average Shares Outstanding                
677674000        672493000        676519000        679612000        682071000        682969000        685851000        687024000        692267000        695193000 698199000                 
Reported Normalized Diluted EPS                                                                                
00010                                 
Basic EPS                
00114        00031        00028        00028        00027        00023        00017        00010        00010        00015        00010                00001 
Diluted EPS               
00112        00031        00028        00027        00026        00022        00016        00010        00010        00015        00010                 
Basic WASO                
667650000        662664000        665758000        668958000        673220000        675581000        679449000        681768000        686465000        688804000        692741000                 
# Diluted WASO                
677674000        672493000        676519000        679612000        682071000        682969000        685851000        687024000        692267000        695193000        698199000                 
Fiscal year end September 28th., 2022. | USD
