"""
solace_common.py

Shared Solace connection setup used by both publisher.py and
subscriber.py. Keeping this in one place means both sides agree on
the same broker details and topic naming, whether they run in the
same process or on completely separate machines.

Connection details come from environment variables, so you can point
publisher.py and subscriber.py at the same broker even when they run
on different hosts:

    SOLACE_HOST      e.g. tcp://mybroker.example.com:55555
    SOLACE_VPN
    SOLACE_USERNAME
    SOLACE_PASSWORD

If unset, they fall back to a local PubSub+ software broker, which is
only reachable if publisher/subscriber are on the same machine/network
as that broker.
"""

import os

from solace.messaging.messaging_service import (
    MessagingService,
    ReconnectionListener,
    ReconnectionAttemptListener,
    ServiceInterruptionListener,
    RetryStrategy,
    ServiceEvent,
)

# Both publisher and subscriber need to agree on this to find each
# other's messages.
TOPIC_PREFIX = "solace/samples"

# ---------------------------------------------------------------------
# SGX ticker universe
#
# This is the full set of SGX-listed codes the dashboard's dropdown can
# pick from — NOT the set the publisher actively polls. With 500
# tickers, polling all of them on a fixed interval would never keep up
# and would risk getting rate-limited by Yahoo Finance. Instead, the
# publisher only polls tickers a dashboard is actually watching right
# now (see the active-ticker set in publisher.py); this list here is
# purely for populating the dropdown and looking up display names.
#
# Only ticker code + company name are kept — market cap / price /
# change / volume from whatever snapshot this list was built from
# would be stale the moment the app starts, so they aren't stored
# here; real numbers come from the live yfinance polling pipeline.
#
# Format: "CODE<TAB>Company Name", one per line. yfinance identifies
# SGX-listed securities with a ".SI" suffix, which is appended when
# this is parsed below.
# ---------------------------------------------------------------------
_SGX_RAW_LISTING = """\
UXSD	Space Exploration Technologies Corp.
HTCD	Tencent Holdings Limited
HSHD	HSBC Holdings plc
HPCD	PetroChina Company Limited
HBBD	Alibaba Group Holding Limited
HBND	Bank of China Limited
HCCD	Contemporary Amperex Technology Co., Limited
HCMD	China Mobile Limited
D05	DBS Group Holdings Ltd
HPAD	Ping An Insurance (Group) Company of China, Ltd.
HSMD	Semiconductor Manufacturing International Corporation
HYDD	BYD Company Limited
TDED	Delta Electronics (Thailand) Public Company Limited
O39	Oversea-Chinese Banking Corporation Limited
HXXD	Xiaomi Corporation
HMTD	Meituan
UGGD	Sea Limited
Z74	Singapore Telecommunications Limited
Z77	Singapore Telecommunications Limited
U11	United Overseas Bank Limited
IBKD	PT Bank Central Asia Tbk
HJDD	JD.com, Inc.
HZGD	Zijin Gold International Company Limited
HBUD	Baidu, Inc.
K6S	Prudential plc
TADD	Advanced Info Service Public Company Limited
TGUD	Gulf Development Public Company Limited
HTGD	Trip.com Group Limited
TATD	Airports of Thailand Public Company Limited
HPPD	Pop Mart International Group Limited
S63	Singapore Technologies Engineering Ltd
HGMD	Geely Automobile Holdings Limited
HKUD	Kuaishou Technology
S68	Singapore Exchange Limited
J36	Jardine Matheson Holdings Limited
F34	Wilmar International Limited
C6L	Singapore Airlines Limited
Q0F	IHH Healthcare Berhad
TPED	PTT Exploration and Production Public Company Limited
H78	Hongkong Land Holdings Limited
TKKD	Kasikornbank Public Company Limited
BN4	Keppel Ltd.
C38U	CapitaLand Integrated Commercial Trust
ITKD	Perusahaan Perseroan (Persero) PT Telekomunikasi Indonesia Tbk
G07	Great Eastern Holdings Limited
UGBD	Grab Holdings Limited
TCPD	CP ALL Public Company Limited
BS6	Yangzijiang Shipbuilding (Holdings) Ltd.
SO7	Yangzijiang Shipbuilding (Holdings) Ltd.
NIO	NIO Inc.
A17U	CapitaLand Ascendas REIT
9CI	CapitaLand Investment Limited
TSCD	The Siam Cement Public Company Limited
TBDD	Bangkok Dusit Medical Services Public Company Limited
HLPD	Laopu Gold Co., Ltd.
Y92	Thai Beverage Public Company Limited
HHZD	Horizon Robotics
C07	Jardine Cycle & Carriage Limited
U96	Sembcorp Industries Ltd
U14	UOL Group Limited
G13	Genting Singapore Limited
S58	SATS Ltd.
5E2	Seatrium Limited
N2IU	Mapletree Pan Asia Commercial Trust
C09	City Developments Limited
HUUD	Ubtech Robotics Corp Ltd
TPFD	Charoen Pokphand Foods Public Company Limited
D01	DFI Retail Group Holdings Limited
M44U	Mapletree Logistics Trust
IICD	PT Indofood CBP Sukses Makmur Tbk
AJBU	Keppel DC REIT
EB5	First Resources Limited
ME8U	Mapletree Industrial Trust
EMI	Emperador Inc.
OV8	Sheng Siong Group Ltd
8A8	China Medical System Holdings Limited
T14	Tianjin Pharmaceutical Da Ren Tang Group Corporation Limited
VC2	Olam Group Limited
U06	Singapore Land Group Limited
V03	Venture Corporation Limited
J69U	Frasers Centrepoint Trust
T82U	Suntec Real Estate Investment Trust
K71U	Keppel REIT
TQ5	Frasers Property Limited
U10	UOB-Kay Hian Holdings Limited
CJLU	NetLink NBN Trust
BUOU	Frasers Logistics & Commercial Trust
S59	SIA Engineering Company Limited
H02	Haw Par Corporation Limited
E5H	Golden Agri-Resources Ltd
HMN	CapitaLand Ascott Trust
AVP	AvePoint, Inc.
A7RU	Keppel Infrastructure Trust
P8Z	Bumitama Agri Ltd.
AWX	AEM Holdings Ltd.
XWA	AEM Holdings Ltd.
C52	ComfortDelGro Corporation Limited
AIY	iFAST Corporation Ltd.
C2PU	Parkway Life Real Estate Investment Trust
F17	GuocoLand Limited
H15	Hotel Properties Limited
S07	Shangri-La Asia Limited
558	UMS Integration Limited
H22	Hong Leong Asia Ltd.
NS8U	Hutchison Port Holdings Trust
8YZ	Yangzijiang Maritime Development Ltd.
F99	Fraser and Neave, Limited
TS0U	OUE Real Estate Investment Trust
8C8U	Centurion Accommodation REIT
9A4U	ESR-REIT
JYEU	Lendlease Global Commercial REIT
CC3	StarHub Ltd
AGS	The Hour Glass Limited
BSL	Raffles Medical Group Ltd
UGS	UltraGreen.ai Limited
ULG	UltraGreen.ai Limited
BVA	Top Glove Corporation Bhd.
F03	Food Empire Holdings Limited
CY6U	CapitaLand India Trust
G92	China Aviation Oil (Singapore) Corporation Ltd
A50	Thomson Medical Group Limited
O5RU	AIMS APAC REIT
H13	Ho Bee Land Limited
OU8	Centurion Corporation Limited
OYY	PropNex Limited
P15	Pacific Century Regional Developments Limited
CWBU	Stoneweg Europe Stapled Trust
CWCU	Stoneweg Europe Stapled Trust
SET	Stoneweg Europe Stapled Trust
Z25	Yanlord Land Group Limited
B61	Bukit Sembawang Estates Limited
P40U	Starhill Global Real Estate Investment Trust
AP4	Riverstone Holdings Limited
NTDU	NTT DC REIT
W05	Wing Tai Holdings Limited
Q5T	Far East Hospitality Trust
E28	Frencken Group Limited
BEC	BRC Asia Limited
UIBU	UI Boustead Real Estate Investment Trust
AU8U	CapitaLand China Trust
S61	SBS Transit Ltd
NC2	Sri Trang Agro-Industry Public Company Limited
ADN	First Sponsor Group Limited
S41	Hong Leong Finance Limited
STG	Sri Trang Gloves (Thailand) Public Company Limited
P52	Pan-United Corporation Ltd
F9D	Boustead Singapore Limited
EH5	United Overseas Australia Ltd
PCT	PC Partner Group Limited
FHH	Foundation Healthcare Holdings Limited
J85	CDL Hospitality Trusts
T6I	ValueMax Group Limited
RE4	Geo Energy Resources Limited
544	CSE Global Limited
CRPU	Sasseur Real Estate Investment Trust
DCRU	Digital Core REIT
BWM	Zheneng Jinjiang Environment Holding Company Limited
S08	Singapore Post Limited
P9D	Civmec Limited
YF8	Yangzijiang Financial Holding Ltd.
5WJ	MoneyMax Financial Services Ltd.
LJ3	OUE Limited
S20	The Straits Trading Company Limited
5UF	Aspial Lifestyle Limited
H07	Stamford Land Corporation Ltd
CHZ	HRnetGroup Limited
MZH	Nanofilm Technologies International Limited
H30	Hong Fok Corporation Limited
HQU	Oiltek International Limited
C41	Cortina Holdings Limited
8AZ	Aztech Global Ltd.
U9E	China Everbright Water Limited
QES	China Sunsine Chemical Holdings Ltd.
WJP	VICOM Ltd
E3B	Wee Hur Holdings Ltd.
M1GU	Alpha Integrated Real Estate Investment Trust
Q01	QAF Limited
O10	Far East Orchard Limited
P34	Delfi Limited
A31	Addvalue Technologies Ltd
U13	United Overseas Insurance Limited
5LY	Marco Polo Marine Ltd.
T15	Tan Chong International Limited
H18	Hotel Grand Central Limited
5TP	CNMC Goldmine Holdings Limited
QC7	Q & M Dental Group (Singapore) Limited
S56	Samudera Shipping Line Ltd
5JS	Indofood Agri Resources Ltd.
F83	COSCO SHIPPING International (Singapore) Co., Ltd.
NPW	Malaysia Smelting Corporation Berhad
1MZ	Nam Cheong Limited
AW9U	First Real Estate Investment Trust
1D0	Kimly Limited
TSH	TSH Resources Berhad
B58	Banyan Tree Holdings Limited
BN2	Valuetronics Holdings Limited
SEG	Concord New Energy Group Limited
42C	iX Biopharma Ltd.
MV4	Mewah International Inc.
BHK	SIIC Environment Holdings Ltd.
ZQM	Soilbuild Construction Group Ltd.
ZKX	Ever Glory United Holdings Limited
5GD	Sunpower Group Ltd.
5CF	OKP Holdings Limited
ODBU	United Hampshire US Real Estate Investment Trust
BDX	GSH Corporation Limited
5HV	Koh Brothers Eco Engineering Limited
M01	Metro Holdings Limited
KUO	International Cement Group Ltd.
S3N	Global Resource Construction Ltd.
B28	Bonvests Holdings Limited
T24	Tuan Sing Holdings Limited
5DD	Micro-Mechanics (Holdings) Ltd.
S35	Sing Investments & Finance Limited
HLS	Helens International Holdings Company Limited
Y03	Yeo Hiap Seng Limited
A04	ASL Marine Holdings Ltd.
MENU	Elite UK REIT
MXNU	Elite UK REIT
DHLU	Daiwa House Logistics Trust
AWZ	Multi-Chem Limited
BTM	Penguin International Limited
N02	NSL Ltd
5IG	Gallant Venture Ltd.
JCO	JustCo Holdings Limited
5UX	Oxley Holdings Limited
I07	ISDN Holdings Limited
BTE	Bund Center Investment Ltd
A30	Aspial Corporation Limited
BBW	Azeus Systems Holdings Ltd.
5JK	Hiap Hoe Limited
OXMU	Prime US REIT
1D4	Aoxin Q & M Dental Group Limited
S85	Straco Corporation Limited
G20	GP Industries Limited
AWI	Thakral Corporation Ltd
QNS	Southern Alliance Mining Ltd.
H12	Hotel Royal Limited
BEW	JB Foods Limited
ITS	Info-Tech Systems Ltd.
TCU	Credit Bureau Asia Limited
NR7	Raffles Education Limited
CLN	APAC Realty Limited
BQF	XMH Holdings Ltd.
UD1U	IREIT Global
41O	LHN Limited
BQM	Tiong Woon Corporation Holding Ltd
CMOU	KORE US REIT
500	Tai Sin Electric Limited
X5N	Avarga Limited
5VS	Hafary Holdings Limited
W8W	Coliwoo Holdings Limited
LCC	Lum Chang Creations Limited
1J4	JEP Holdings Ltd.
DM0	PSC Corporation Ltd.
5IC	Sing Holdings Limited
BMGU	BHG Retail REIT
C33	Chuan Hup Holdings Limited
MR7	Nordic Group Limited
DU4	Mermaid Maritime Public Company Limited
L19	Lum Chang Holdings Limited
Z59	Yoma Strategic Holdings Ltd.
40T	ISEC Healthcare Ltd.
ER0	KSH Holdings Limited
SHE	Soon Hock Enterprise Holding Limited
9MT	MetaOptics Ltd
42R	Jumbo Group Limited
XZL	Acrophyte Hospitality Trust
T12	Tat Seng Packaging Group Ltd
G0I	Nam Lee Pressed Metal Industries Limited
LVR	17LIVE Group Limited
BLS	Hotung Investment Holdings Limited
S7OU	Asian Pay Television Trust
J2T	Hock Lian Seng Holdings Limited
T13	RH PetroGas Limited
S44	EnGro Corporation Limited
5ML	Old Chang Kee Ltd.
M14	InnoTek Limited
5WA	OUE Healthcare Limited
BTG	HG Metal Manufacturing Limited
Q0X	Ley Choon Group Holdings Limited
G50	Grand Banks Yachts Limited
D03	Del Monte Pacific Limited
41B	Huationg Global Limited
K75	Koh Brothers Group Limited
BNE	Kencana Agri Limited
1F2	Union Gas Holdings Limited
OAJ	Fortress Minerals Limited
ZBA	Infinity Development Holdings Company Limited
S23	Singapura Finance Ltd
BCY	Powermatic Data Systems Limited
BEZ	Beng Kuang Marine Limited
S19	Singapore Shipping Corporation Limited
5DP	Heeton Holdings Limited
BTOU	Manulife US Real Estate Investment Trust
L02	Metis Energy Limited
TKU	Toku Ltd.
42L	Taka Jewellery Holdings Limited
BDR	Willas-Array Electronics (Holdings) Limited
V7R	Resources Global Development Limited
BPF	YHI International Limited
5MZ	Kingsmen Creatives Ltd.
RXS	Pacific Radiance Ltd.
T41	TeleChoice International Limited
1J5	Hyphens Pharma International Limited
BQD	Envictus International Holdings Limited
HKB	AMTD IDEA Group
5WH	Rex International Holding Limited
D5IU	Landmark REIT
BTP	Baker Technology Limited
575	ASTI Holdings Limited
42E	Choo Chiang Holdings Ltd.
F86	MYP Ltd.
KJ5	BBR Holdings (S) Ltd
NXR	iWOW Technology Limited
BIP	Vibrant Group Limited
WKS	Winking Studios Limited
PPC	ProsperCap Corporation Limited
N08	New Toyo International Holdings Ltd
TAP	The Assembly Place Holdings Ltd.
42T	The Trendlines Group Ltd.
Y3D	mDR Limited
C9Q	Sinostar PEC Holdings Limited
5SO	Duty Free International Limited
533	ABR Holdings Limited
BHU	SUTL Enterprise Limited
O9E	Parkson Retail Asia Limited
Y35	AnAn International Limited
XJB	G.H.Y Culture & Media Holding Co., Limited
5SR	Zhongmin Baihui Retail Group Ltd.
5AE	Pollux Properties Ltd.
5AB	Trek 2000 International Ltd
566	SHS Holdings Ltd.
CHJ	Uni-Asia Group Limited
F13	Fu Yu Corporation Limited
D8DU	First Ship Lease Trust
8YY	Embracing Future Holdings Limited
URR	Sim Leisure Group Ltd.
S69	Serial System Ltd
N01	Nera Telecommunications Ltd
S71	Sunright Limited
RQ1	Overseas Education Limited
5DS	Megachem Limited
NEX	Reclaims Global Limited
595	GKE Corporation Limited
42W	Zixin Group Holdings Limited
I49	IFS Capital Limited
BKX	Yongmao Holdings Limited
546	Medtecs International Corporation Limited
BKA	Sin Heng Heavy Machinery Limited
U77	Sarine Technologies Ltd.
1B1	HC Surgical Specialists Limited
5UL	Atlantic Navigation Holdings (Singapore) Limited
5WF	ISOTeam Ltd.
40V	Alset International Limited
WPC	Vallianz Holdings Limited
GEH	Goodwill Entertainment Holding Limited
BBP	Hor Kew Corporation Limited
BFT	Lincotrade & Associates Holdings Limited
1L2	Hiap Seng Industries Limited
LMS	LMS Compliance Ltd.
YK9	YKGI Limited
UIX	China Environmental Resources Group Limited
5G2	Kim Heng Limited
C76	Creative Technology Ltd
ZB9	Union Steel Holdings Limited
K29	Karin Technology Holdings Limited
1AZ	Audience Analytics Limited
1E3	Sanli Environmental Limited
43A	Octopus (APAC) Holdings Limited
A55	Asia Enterprises Holding Limited
FQ7	Salt Investments Limited
PA3	TA Corporation Ltd
579	Oceanus Group Limited
53W	Attika Group Ltd.
8K7	UG Healthcare Corporation Limited
T43	Yunnan Energy International Co. Limited
5I1	KOP Limited
BTJ	A-Sonic Aerospace Limited
42S	Astaka Holdings Limited
T55	TIH Limited
9G2	Singapore Institute of Advanced Medicine Holdings Ltd.
B49	World Precision Machinery Limited
M05	MTQ Corporation Limited
O08	Ossia International Limited
1A1	Wong Fong Industries Limited
5DM	Ying Li International Real Estate Limited
S29	Stamford Tyres Corporation Limited
XZB	Skylink Holdings Limited
BIX	Ellipsiz Ltd
S9B	Amcorp Global Limited
Y8E	Samurai 2K Aerosol Limited
5PC	Goodland Group Limited
1R6	Avi-Tech Holdings Limited
C8R	Jiutian Chemical Group Limited
C06	CSC Holdings Limited
5NV	Chasen Holdings Limited
5NF	Mencast Holdings Ltd.
1V3	Mooreast Holdings Ltd.
L23	Enviro-Hub Holdings Ltd.
42F	TOTM Technologies Limited
BEI	LHT Holdings Limited
C05	Chemical Industries (Far East) Limited
569	Vicplas International Ltd
UZF	Dezign Format Group Limited
N0Z	Combine Will International Holdings Limited
1Y1	9R Limited
AYN	Global Testing Corporation Limited
504	HS Optimus Holdings Limited
XVG	Aedge Group Limited
5LE	Sitra Holdings (International) Limited
508	Fuji Offset Plates Manufacturing Ltd
554	King Wan Corporation Limited
AVX	HL Global Enterprises Limited
I06	Intraco Limited
KIN	Kin Global Limited
CTO	Hong Lai Huat Group Limited
5F7	Wilton Resources Corporation Limited
5PO	Hiap Tong Corporation Ltd.
5EG	Zhongxin Fruit and Juice Limited
P8A	Cordlife Group Limited
1J7	Jawala Inc.
BDA	PNE Industries Ltd
MIJ	Alliance Healthcare Group Limited
BFI	Tiong Seng Holdings Limited
541	Abundance International Limited
SGR	Sheffield Green Ltd.
1F3	Aspen (Group) Holdings Limited
1C0	Emerging Towns & Cities Singapore Ltd.
5TT	Keong Hong Holdings Limited
DRX	ST Group Food Industries Holdings Limited
5PD	Hengyang Petrochemical Logistics Limited
R14	Eneco Energy Limited
VIN	Vin's Holdings Ltd
Z4D	Medi Lifestyle Limited
OTX	Medinex Limited
OTS	OTS Holdings Limited
5GZ	HGH Holdings Ltd.
CNE	MindChamps PreSchool Limited
BDU	Federal International (2000) Ltd
CIN	Courage Investment Group Limited
43B	Secura Group Limited
A33	Southern Archipelago Ltd.
BQN	BH Global Corporation Limited
E6R	Le tree Holdings Limited
596	Pavillon Holdings Ltd.
BQC	A-Smart Holdings Ltd.
C13	CH Offshore Ltd.
J03	Jadason Enterprises Ltd
BJZ	Koda Ltd
5SY	OneApex Limited
BFU	Tye Soon Limited
43Q	Advancer Global Limited
505	AsiaMedic Limited
BKW	Datapulse Technology Limited
C04	Casa Holdings Limited
P36	Pan Hong Holdings Group Limited
1D1	UnUsUaL Limited
5GI	Interra Resources Limited
LGH	Leong Guan Holdings Limited
GRQ	UpHealth Group Limited
AWG	Ascent Bridge Limited
BQP	Southern Packaging Group Limited
K03	Khong Guan Limited
AWC	Brook Crompton Holdings Ltd.
5MD	Soon Lian Holdings Limited
YSV	Khen Energy Limited
5AI	H2G Green Limited
5AU	AP Oil International Limited
5EV	Hosen Group Ltd.
AJ2	Ouhua Energy Holdings Limited
40W	ZICO Holdings Inc.
QZG	Accrelist Ltd.
543	Noel Gifts International Ltd
570	Abundante Limited
BTX	Anchun International Holdings Ltd.
BAZ	Lion Asiapac Limited
XHV	Serial Achieva Limited
1B0	mm2 Asia Ltd.
5IF	Natural Cool Holdings Limited
CYW	TrickleStar Limited
5OI	Japan Foods Holding Ltd.
BXE	CDW Holding Limited
BRD	Sapphire Corporation Limited
1H8	LY Corporation Limited
AAJ	Sunmoon Food Company Limited
LS9	Leader Environmental Technologies Limited
A52	AnnAik Limited
AOF	Amplefield Limited
5RA	Asia-Pacific Strategic Investments Limited
5PF	Jason Marine Group Limited
AWK	Fuxing China Group Limited
E27	The Place Holdings Limited
5BI	Polaris Ltd.
5VC	Kori Holdings Limited
CJN	British and Malayan Holdings Limited
KYB	Food Innovators Holdings Limited
5VP	GDS Global Limited
PRH	Livingstone Health Holdings Limited
MF6	Mun Siong Engineering Limited"""


def _parse_sgx_listing(raw_listing):
    """Turn _SGX_RAW_LISTING into (ordered ticker list, TICKER_INFO dict)."""
    tickers = []
    info = {}
    for line in raw_listing.strip().splitlines():
        code, _, name = line.partition("\t")
        if not code or not name:
            continue
        symbol = f"{code}.SI"
        tickers.append(symbol)
        info[symbol] = {"name": name, "exchange": "SGX", "currency": "SGD"}
    return tickers, info


# The full dropdown universe (500 SGX-listed codes) and their display
# info, both derived from _SGX_RAW_LISTING above.
ALL_SGX_TICKERS, TICKER_INFO = _parse_sgx_listing(_SGX_RAW_LISTING)

# Kept under its old name too since dashboard.py/subscriber.py/main.py
# already import AVAILABLE_TICKERS for "the tickers the dropdown
# offers" — same meaning as before, just backed by the full SGX list
# now instead of a hand-picked pair.
AVAILABLE_TICKERS = ALL_SGX_TICKERS

# What the subscriber/dashboard start on before the user picks
# anything — DBS, same as the original default.
DEFAULT_TICKER = "D05.SI" if "D05.SI" in TICKER_INFO else ALL_SGX_TICKERS[0]


def topic_for_ticker(ticker_symbol):
    """
    The topic a given ticker's ticks are published/subscribed on.
    One topic per ticker (no sequence number in the path) so the
    subscriber can subscribe to exactly one stock at a time.
    """
    return f"{TOPIC_PREFIX}/python/stocks/{ticker_symbol}"


# Topic namespace used to tell the publisher to (a) replay a ticker's
# day-so-far history on demand and start actively polling it ("start"),
# or (b) stop actively polling a ticker the dashboard has switched
# away from ("stop") — see publisher.py's active-ticker set, which
# exists because polling the entire ~500-ticker SGX universe every
# cycle wouldn't keep up or would risk yfinance rate-limiting.
#
# Both directions share one topic per ticker; the message payload
# carries {"ticker": ..., "action": "start"|"stop"} rather than using
# two separate topics, since a single Direct Receiver only dispatches
# to one MessageHandler anyway — the action field is what the handler
# branches on.
BACKFILL_REQUEST_TOPIC_PREFIX = f"{TOPIC_PREFIX}/python/backfill-request"


def backfill_request_topic(ticker_symbol):
    return f"{BACKFILL_REQUEST_TOPIC_PREFIX}/{ticker_symbol}"


def topic_for_news(ticker_symbol):
    """
    The topic a given ticker's news articles are published/subscribed
    on. Separate namespace from price ticks (different message shape,
    much lower frequency) but same one-topic-per-ticker convention, so
    the subscriber can subscribe to just the news for whichever stock
    it's currently showing.
    """
    return f"{TOPIC_PREFIX}/python/news/{ticker_symbol}"


def get_broker_props():
    return {
        "solace.messaging.transport.host": os.environ.get("SOLACE_HOST") or "tcp://localhost:55554",
        "solace.messaging.service.vpn-name": os.environ.get("SOLACE_VPN") or "default",
        "solace.messaging.authentication.scheme.basic.username": os.environ.get("SOLACE_USERNAME") or "admin",
        "solace.messaging.authentication.scheme.basic.password": os.environ.get("SOLACE_PASSWORD") or "admin",
    }


def build_messaging_service():
    """Build and (blocking) connect a MessagingService using the shared broker props."""
    messaging_service = (
        MessagingService.builder()
        .from_properties(get_broker_props())
        .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3))
        .build()
    )
    messaging_service.connect()
    return messaging_service


class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent):
        print("\non_reconnected")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

    def on_reconnecting(self, e: "ServiceEvent"):
        print("\non_reconnecting")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

    def on_service_interrupted(self, e: "ServiceEvent"):
        print("\non_service_interrupted")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")


def attach_service_listeners(messaging_service):
    """Wire up the standard reconnection/interruption logging listeners."""
    handler = ServiceEventHandler()
    messaging_service.add_reconnection_listener(handler)
    messaging_service.add_reconnection_attempt_listener(handler)
    messaging_service.add_service_interruption_listener(handler)
    return handler
