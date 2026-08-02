//! Static Polygon token symbols for oracle audit / demand (DexScreener-verified where noted).

use alloy::primitives::Address;
use alloy::primitives::address;

#[must_use]
pub fn lookup_symbol(addr: &Address) -> Option<&'static str> {
    KNOWN_POLYGON_SYMBOLS
        .iter()
        .find(|(a, _)| *a == addr)
        .map(|(_, sym)| *sym)
}

/// Top PG + runtime demand addresses (DexScreener polygon pairs, 2026-07).
const KNOWN_POLYGON_SYMBOLS: &[(&Address, &str)] = &[
    (
        &address!("0xfB021f1Ce0d5327eE31EFE21F9b23c0b9A9ccC58"),
        "fTIME",
    ),
    (
        &address!("0x1deFA61862E27E5C9E8b9f8daE769290B9CF6D93"),
        "ELONINDEX",
    ),
    (
        &address!("0x35aC92580eb672d9c76FeE4228165F6eF224c334"),
        "ELONINDEX",
    ),
    (
        &address!("0xB29781B8B09ba707631eE6720E50129777d4ba31"),
        "PFC",
    ),
    (
        &address!("0x0E9b89007eEE9c958c0EDA24eF70723C2C93dD58"),
        "ankrMATIC",
    ),
    (
        &address!("0x5118aeC3AfCca3f1e21733eE9C88BB800AFE6F7b"),
        "FLDG",
    ),
    (
        &address!("0x86233fF3eD561A0E35910920acB2D91419Fd14d0"),
        "DOGE",
    ),
    (
        &address!("0x9Cb74C8032b007466865f060ad2c46145d45553D"),
        "IDEX",
    ),
    (
        &address!("0xa8c17604f6C9fFA20955b40CC3B607Bf9606b625"),
        "DOGEINDEX",
    ),
    (
        &address!("0xEe9A352F6aAc4aF1A5B9f467F6a93E0ffBe9Dd35"),
        "MASQ",
    ),
    (
        &address!("0x02753ee6a439BE76cFAF828CeBE02ce88eCFf8b5"),
        "NOW",
    ),
    (
        &address!("0xe1398B5d2f3CEF77a13a7CcBae33F2121c217301"),
        "RODO",
    ),
    (
        &address!("0x00e5646f60AC6Fb446f621d146B6E1886f002905"),
        "RAI",
    ),
    (
        &address!("0x454551c6027941BA34e149CC92A8dfC30c191570"),
        "EFP",
    ),
    (
        &address!("0x64c1B62Fe216B167BC7fd5B217630d494954b4c4"),
        "ELONINDEX",
    ),
    (
        &address!("0x8031c44b96Ec8c9B66aB16c2c164e8dEEb361a3f"),
        "LIRA",
    ),
    (
        &address!("0xB0ac8D198088cf310F45d9d05C747263be994849"),
        "UCTL",
    ),
    (
        &address!("0x5a5d62660F5B0556d33513869a4C9bA97d2d88e4"),
        "CTK multi",
    ),
    (
        &address!("0x62F594339830b90AE4C084aE7D223fFAFd9658A7"),
        "SPHERE",
    ),
    (
        &address!("0xfe48ee5070a045B2Caa2Ce9e344735D9E4886B46"),
        "3TK",
    ),
    (
        &address!("0x1c67201D5B748C9666205c2a1f7E2d4272F5Da3F"),
        "ENTER",
    ),
    (
        &address!("0x61fFE097137d543f019F5257E1a1Ff7A6C5F0b68"),
        "UNI",
    ),
    (
        &address!("0x831753DD7087CaC61aB5644b308642cc1c33Dc13"),
        "QUICK",
    ),
    (
        &address!("0xB5C064F955D8e7F38fE0460C556a72987494eE17"),
        "QUICKv2",
    ),
    (
        &address!("0xA571963278014B5B3A686778747fDf8ad4dFBb94"),
        "SD",
    ),
    (
        &address!("0x3d2bD0e15829AA5C362a4144FdF4A1112fa29B5c"),
        "FBX",
    ),
    (
        // on-chain: SP / STAR-Power (was mislabeled JPYC from demand noise)
        &address!("0x72d31b6dD46DaaE07391036097A2CB4648991eCD"),
        "SP",
    ),
    (
        // on-chain: USD-SS / USD Staked DeFi LP (StarSeeds) — not BANANA
        &address!("0x4b0dF7EDe79be6b046a4Ed71580A3733A109e641"),
        "USD-SS",
    ),
    (
        &address!("0xbbC11D55375F0B37f8A30b102C9ce143B097671e"),
        "SUSHI",
    ),
    (
        &address!("0x3A58a54C066FdC0f2D55FC9C89F0415C92eBf3C4"),
        "stMATIC",
    ),
    (
        &address!("0x3A58a54C066FdC0f2D55FC9C89F0415A6B4066ff"),
        "stMATIC",
    ),
    (
        &address!("0xfa68FB4628DFF1028CFEc22b4162FCcd0d45efb6"),
        "MaticX",
    ),
    (
        &address!("0xFa68FB4628dFF1028C0C610198bB4D9B5AfE0902"),
        "MaticX",
    ),
    (
        &address!("0xa3Fa99A148fA48D14Ed51d610c367C61876997F1"),
        "miMATIC",
    ),
    (
        &address!("0x5Dd05762b831A977B974Db8759772D41F3D5Ff0b"),
        "FCD",
    ),
    (
        &address!("0x82a0E6c02b91eC9f6ff943C0A933c03dBaa19689"),
        "WETH",
    ),
    (
        &address!("0xF32E6dC7709c596c5a5f328fa01eDd8eC3F62517"),
        "EURA",
    ),
    (
        &address!("0x3A29CAb2E124919d14a6F735b6033a3AaD2B260F"),
        "GNS",
    ),
    (
        // Aavegotchi GHST (not Gains GNS — that is 0xE5417…).
        &address!("0x385Eeac5cB85A38A9a07A70c73e0a3271CfB54A7"),
        "GHST",
    ),
    (
        &address!("0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD"),
        "wstETH",
    ),
    (
        &address!("0x63d38FCf3cC014735B28339F47EC3FA9BA97b4B9"),
        "miMATIC",
    ),
    (
        &address!("0x692597b009d13C4049a947CAB2239b7d6517875F"),
        "ADDY",
    ),
    (
        &address!("0xC3C7d422809852031b44ab29EEC9F1EfF2A58756"),
        "DOLA",
    ),
    (
        // on-chain: KOR / Kora (was mislabeled jGBP)
        &address!("0x0d929e52EFBf26F2322fd4033B157538c3b80474"),
        "KOR",
    ),
    (
        &address!("0x6f7C932e7684666C9fd1d44527765433e01fF61d"),
        "miMATIC",
    ),
    (
        &address!("0x553d3D295e0f695B9228246232eDF400ed3560B5"),
        "PAXG",
    ),
    (
        &address!("0xcE20F7cb738aA5Cf32441B2ba0EFBA1E6f42c0b4"),
        "sUSD",
    ),
    (&address!("0x6631eE651DA438Db2BE611B5A44dFE2Ca04590C5"), "A"),
    (
        &address!("0xF5c068f28eBF91b22e52C2ecD230621879e914B8"),
        "ALA",
    ),
    (
        &address!("0xeB51D9A39AD5EEF215dC0Bf39a8821ff804A0F01"),
        "LGNS",
    ),
    (
        &address!("0x6f8a06447Ff6FcF75d803135a7de15CE88C1d4ec"),
        "SHIB",
    ),
    (
        &address!("0xBbba073C31bF03b8ACf7c28EF0738DeCF3695683"),
        "SAND",
    ),
    (
        &address!("0x50B728D8D964fd00C2d0AAD81718b71311feF68a"),
        "SNX",
    ),
    (
        &address!("0x43635fe8B19551B8Bc6eF2959989d481Cf464f02"),
        "deUSDC",
    ),
    (
        &address!("0xA1F700a822f8B70c62F795Fd74b57EC6131F4A85"),
        "miMATIC",
    ),
    (
        &address!("0x2C89bbc92BD86F8075d1DEcc58C7F4E0107f286b"),
        "AVAX",
    ),
    (
        &address!("0xd93f7E271cB87c23AaA73edC008A79646d1F9912"),
        "SOL",
    ),
    (
        &address!("0x99a57E6C8558BC6689f894e068733ADf83C19725"),
        "sLGNS",
    ),
    (
        &address!("0xE0339c80fFDE91F3e20494Df88d4206D86024cdF"),
        "ELON",
    ),
    (
        &address!("0x5536dac6C2F50746cFCa393ea4F23C18d56f7dcf"),
        "GLD",
    ),
    (
        &address!("0x081Ec4c0e30159C8259BAD8F4887f83010a681DC"),
        "DE",
    ),
    (
        &address!("0xC4533c9d6b76E43fd87f03285cEc43e6C3248190"),
        "APS",
    ),
    (
        &address!("0x255707B70BF90aa112006E1b07B9AeA6De021424"),
        "TETU",
    ),
    (
        &address!("0x46d3EC8CE3eC767414F16FE12176De23E3E5B46A"),
        "SXC",
    ),
    (
        &address!("0x73b29199a8e4C146E893EB95f18dAc41738a88c6"),
        "BAG",
    ),
    (
        &address!("0x24834BBEc7E39ef42f4a75EAF8E5B6486d3F0e57"),
        "LUNA",
    ),
    (
        &address!("0xD5e36D81f686c4dfea52D93A361a0eaccC5cc5De"),
        "ADC",
    ),
    (
        &address!("0x4e78011Ce80ee02d2c3e649Fb657E45898257815"),
        "KLIMA",
    ),
    (
        &address!("0x61299774020dA444Af134c82fa83E3810b309991"),
        "RNDR",
    ),
    (
        &address!("0x2059fe4b81751878A24515B404A2d6Ea12b2aC92"),
        "GHST-SS",
    ),
    (
        &address!("0xa5931BCEba09F8e35eF27aA545B4EAAc6Ad710F8"),
        "STAR-L",
    ),
    (
        &address!("0x6280496ef5565bD8Bc6e1cE83e8cB4078904b7e3"),
        "BCOP",
    ),
    (
        &address!("0x2Da1D58331057033A186e3475A2CD1C2B76C0425"),
        "MNO",
    ),
    (
        &address!("0xe70C59b2B919995a6c1919C549D0Bc14677d1B0D"),
        "FISH",
    ),
    (
        &address!("0x45c32fA6DF82ead1e2EF74d17b76547EDdFaFF89"),
        "FRAX",
    ),
    (
        &address!("0xF8a0D4a4F608709446efc7526F4E479Cf22e54eE"),
        "NGC",
    ),
    (
        &address!("0xE56f260e160A26E6Ace16b3B4D8673573876e33F"),
        "PAPU",
    ),
    (
        &address!("0x7DfF46370e9eA5f0Bad3C4E29711aD50062EA7A4"),
        "SOL",
    ),
    (
        &address!("0x3553f861dEc0257baDA9F8Ed268bf0D74e45E89C"),
        "USDT",
    ),
    (
        &address!("0xee546f831533a913848b72f36a9D5E437F63dbB9"),
        "CCDAO",
    ),
    (
        &address!("0x8b1f836491903743fE51ACd13f2CC8Ab95b270f6"),
        "ACY",
    ),
    (
        &address!("0x4C63DEa5e10f5Ac5932cEC0A427bb3633f0520d1"),
        "ZED",
    ),
    (
        &address!("0xbAFB4E877EA9D13C21461FF7888f83FC5270bBAF"),
        "Pi",
    ),
    (
        &address!("0xf28164A485B0B2C90639E47b0f377b4a438a16B1"),
        "dQUICK",
    ),
    (
        &address!("0xbd4a2a668d6a69Ab963347Bd7cA7a438E034B3f2"),
        "50p",
    ),
    (
        &address!("0xC556Cf22AB6d65D7f0Be0355e80A54Ef9E23F7Bb"),
        "BOP",
    ),
    (
        &address!("0x2AB0e9e4eE70FFf1fB9D67031E44F6410170d00e"),
        "mXEN",
    ),
    (
        &address!("0xA9B8e4BE7e4Fc1B1d28B396af7c4CBa37979bC0F"),
        "ATSX",
    ),
    (
        &address!("0xdF7837DE1F2Fa4631D716CF2502f8b230F1dcc32"),
        "TEL",
    ),
    (
        &address!("0xdC94F8A25C65813564eb7b3Ef46081D0f29f74e9"),
        "DKDEFI",
    ),
    (
        &address!("0x1fBeF12C70Ef97fB8f22C5A2aB6E7580Ce86D14D"),
        "11ELONINDEX",
    ),
    (
        &address!("0x9c2C5fd7b07E95EE044DDeba0E97a665F142394f"),
        "1INCH",
    ),
    (
        &address!("0xD12DC5319808Bb31ba95AE5764def2627d5966CE"),
        "BOOTY",
    ),
    (
        &address!("0x1659fFb2d40DfB1671Ac226A0D9Dcc95A774521A"),
        "DLYCOP",
    ),
    (
        &address!("0x229b1b6C23ff8953D663C4cBB519717e323a0a84"),
        "BLOK",
    ),
    (
        &address!("0xD14E0cd48CF32007D0F0b294Ee3d0b1530D8b04F"),
        "InC",
    ),
    (
        &address!("0x1C954E8fe737F99f68Fa1CCda3e51ebDB291948C"),
        "KNC",
    ),
    (
        &address!("0xe20B9e246db5a0d21BF9209E4858Bc9A3ff7A034"),
        "wBAN",
    ),
    (
        &address!("0x7bF44C2BE2b9bAB23cea3e071A14D93dF9CdEFaf"),
        "USD-S",
    ),
    (
        &address!("0xD70fd809b92E36ac7376A8066F34B80925a31fbB"),
        "CYS",
    ),
    (
        &address!("0x4006CB86cF238aF7FA761B9792Aa02939545E604"),
        "ELONINDEX",
    ),
    (
        &address!("0xdC3aCB92712D1D44fFE15d3A8D66d9d18C81e038"),
        "POLAR",
    ),
    (
        &address!("0xD86b5923F3AD7b585eD81B448170ae026c65ae9a"),
        "IRON",
    ),
    (
        &address!("0xDa294Af752F7897C01D97e9C8D8875caF858E78D"),
        "pEUR",
    ),
    (
        &address!("0x5eC03C1f7fA7FF05EC476d19e34A22eDDb48ACdc"),
        "ZED",
    ),
    (
        &address!("0x97bfa4b212A153E15dCafb799e733bc7d1b70E72"),
        "beQI",
    ),
    (
        &address!("0x9A94965D690298C0086AbA54f0D30DAF4ca806a1"),
        "LLK",
    ),
    (
        &address!("0x8A953CfE442c5E8855cc6c61b1293FA648BAE472"),
        "PolyDoge",
    ),
    (
        &address!("0xA128Ad9940C4D4AD54890cBf20370B2F49204Ee5"),
        "DPLIQXJL",
    ),
    (
        &address!("0x260f5D6AB77f2459C231Baf84bd13bfbfA7521E3"),
        "ORACLE",
    ),
    (
        &address!("0x46E3869AbE8Eb6c777eabB6dCb3cE9F38e3Bfcc6"),
        "SYNTH",
    ),
    (
        &address!("0x8174b243559BB4A2742B6c9b4c4f2070FFfCC467"),
        "THEOS",
    ),
    (
        &address!("0xEfC6951F17327aD83bACdd812B5758818Fefc89e"),
        "Binx",
    ),
    (
        &address!("0xE1334eBeB4B7cf2a32819787135Aed4925B6cf70"),
        "VIRAL",
    ),
    (
        &address!("0x9cd6746665D9557e1B9a775819625711d0693439"),
        "LUNA",
    ),
    (
        &address!("0x28e977157727273243CB072f9c9DE494A1387d5d"),
        "STARV5",
    ),
    (
        &address!("0xe4Bf2864ebeC7B7fDf6Eeca9BaCAe7cDfDAffe78"),
        "DODO",
    ),
    (
        &address!("0x0000000000000000000000000000000000001010"),
        "POL",
    ),
    (
        &address!("0xE06Bd4F5aAc8D0aA337D13eC88dB6defC6eAEefE"),
        "IXT",
    ),
    (
        &address!("0xB0a9C70FBBAF01Fc7B97d15bb7DF1C6c651720b7"),
        "DeHu",
    ),
    (
        &address!("0xaAa5B9e6c589642f98a1cDA99B9D024B8407285A"),
        "TITAN",
    ),
    (
        &address!("0x5fe2B58c013d7601147DcdD68C143A77499f5531"),
        "GRT",
    ),
    (
        &address!("0x2F800Db0fdb5223b3C3f354886d907A671414A7F"),
        "BCT",
    ),
    (
        &address!("0x580A84C73811E1839F75d86d75d88cCa0c241fF4"),
        "QI",
    ),
    (
        &address!("0x7Ecb5699D8E0a6572E549Dc86dDe5A785B8c29BC"),
        "MORI",
    ),
    (
        &address!("0x8f006D1e1D9dC6C98996F50a4c810F17a47fBF19"),
        "NSFW",
    ),
    (
        &address!("0x751f9Ed44FC3bF46F8F22aa2F06B8b121d510A80"),
        "MARI",
    ),
    (
        &address!("0x1D88Ad180743727767f484cFdD8373b09222F6A7"),
        "USDT",
    ),
    (
        &address!("0x6A8Ec2d9BfBDD20A7F5A4E89D640F7E7cebA4499"),
        "MSQ",
    ),
    (
        &address!("0x5647Fe4281F8F6F01E84BCE775AD4b828A7b8927"),
        "MM",
    ),
    (
        &address!("0xeCDCB5B88F8e3C15f95c720C51c71c9E2080525d"),
        "WBNB",
    ),
    (
        &address!("0xdddCa1d1fd4E72B85B7B95f07651AB36B62F69E9"),
        "GROSH",
    ),
    (
        &address!("0xE5417Af564e4bFDA1c483642db72007871397896"),
        "GNS",
    ),
    (
        &address!("0x162539172b53E9a93b7d98Fb6c41682De558a320"),
        "GONE",
    ),
    (
        &address!("0x6AE7Dfc73E0dDE2aa99ac063DcF7e8A63265108c"),
        "JPYC",
    ),
    (
        &address!("0x7D645CBbCAdE2A130bF1bf0528b8541d32D3f8Cf"),
        "ALRTO",
    ),
    (
        &address!("0x432cdbC749FD96AA35e1dC27765b23fDCc8F5cf1"),
        "NIFTSY",
    ),
    (
        &address!("0x4fA43A983466DDA2FcA21dd19c4456A2B1C1b857"),
        "Burner",
    ),
    (
        &address!("0xC3Ec80343D2bae2F8E680FDADDe7C17E71E114ea"),
        "OM",
    ),
    (
        &address!("0xCBf4AB00b6Aa19B4d5D29C7c3508B393a1C01Fe3"),
        "MegaDoge",
    ),
    (
        &address!("0x26d326B1fc702260baeB62334d7c1Da6f1a2C386"),
        "GTPS",
    ),
    (
        &address!("0x4731479cd56E3A55f8207Db9734e3FDB5e136D43"),
        "usd",
    ),
    (
        &address!("0x7ABE9Edf5C544A04dA83e9110CF46DBC4759170c"),
        "WPAY",
    ),
    (
        &address!("0xA486c6BC102f409180cCB8a94ba045D39f8fc7cB"),
        "NEX",
    ),
    (
        &address!("0xFC1898b09C385b3B633E3428f941e0Cbad85d48D"),
        "USDC",
    ),
    (
        &address!("0xE111178A87A3BFf0c8d18DECBa5798827539Ae99"),
        "EURS",
    ),
    (
        &address!("0x8505b9d2254A7Ae468c0E9dd10Ccea3A837aef5c"),
        "COMP",
    ),
    (
        &address!("0x0957DfDE2820196Fe9250d308bCED4fFa1F2f8eb"),
        "RCT",
    ),
    (
        &address!("0x576Cf361711cd940CD9C397BB98C4C896cBd38De"),
        "USDC",
    ),
    (
        &address!("0xE0B52e49357Fd4DAf2c15e02058DCE6BC0057db4"),
        "EURA",
    ),
    (
        &address!("0xE6469Ba6D2fD6130788E0eA9C0a0515900563b59"),
        "UST",
    ),
    (
        &address!("0x5c4b7CCBF908E64F32e12c6650ec0C96d717f03F"),
        "BNB",
    ),
    (
        &address!("0x2e1AD108fF1D8C782fcBbB89AAd783aC49586756"),
        "TUSD",
    ),
    (
        &address!("0x311434160D7537be358930def317AfB606C0D737"),
        "NAKA",
    ),
    (
        &address!("0x5ab6Ccac5065d575311e58C458E696c956fDde7d"),
        "VIRGO",
    ),
    (
        &address!("0xA3f751662e282E83EC3cBc387d225Ca56dD63D3A"),
        "APEPE",
    ),
    (
        &address!("0x3a3Df212b7AA91Aa0402B9035b098891d276572B"),
        "FISH",
    ),
    (
        &address!("0xdAb529f40E671A1D4bF91361c21bf9f0C9712ab7"),
        "BUSD",
    ),
    (
        &address!("0xE6A537a407488807F0bbeb0038B79004f19DDDFb"),
        "BRLA",
    ),
    (
        &address!("0x23D29D30e35C5e8D321e1dc9A8a61BFD846D4C5C"),
        "HEX",
    ),
    (
        &address!("0x431D5dfF03120AFA4bDf332c61A6e1766eF37BDB"),
        "JPYC",
    ),
    (
        &address!("0x5F32AbeeBD3c2fac1E7459A27e1AE9f1C16ccccA"),
        "FAR",
    ),
    (
        &address!("0x9d9F8a6A6aD70D5670B7b5Ca2042c7E106E2fB78"),
        "TRUEHN",
    ),
    (
        &address!("0xD1f9c58e33933a993A3891F8acFe05a68E1afC05"),
        "SFL",
    ),
    (
        &address!("0xe5B49820e5A1063F6F4DdF851327b5E8B2301048"),
        "Bonk",
    ),
    (
        &address!("0xE7a24EF0C5e95Ffb0f6684b813A78F2a3AD7D171"),
        "am3CRV",
    ),
    (
        &address!("0x7e9928aFe96FefB820b85B4CE6597B8F660Fe4F4"),
        "oBNB",
    ),
    (
        &address!("0x85955046DF4668e1DD369D2DE9f3AEB98DD2A369"),
        "DPI",
    ),
    (
        &address!("0xE261D618a959aFfFd53168Cd07D12E37B26761db"),
        "DIMO",
    ),
    (
        &address!("0xAC0F66379A6d7801D7726d5a943356A172549Adb"),
        "GEOD",
    ),
    (
        &address!("0x3ce1327867077B551ae9A6987bF10C9fd08edCE1"),
        "SWCH",
    ),
    (
        &address!("0xB1D58054B351A4fbB327CAc2296e3A2e93BBe7E5"),
        "Reaper",
    ),
    (
        &address!("0xCEc974B72997B921F629daE516b890B72Aa0bECa"),
        "Wavy",
    ),
    (
        &address!("0x3Dbd2A88627566306AE9f5F5FB466B498535aF21"),
        "ETHV",
    ),
    (
        &address!("0xAba308c24952f70a533474653349a68Bb639FA2A"),
        "MATIC-SS",
    ),
    (
        &address!("0xD289c01528921B5f6D5B111a50a99456D495bF78"),
        "STARV2",
    ),
    (
        &address!("0xb7Acf75C942D4fB7BD9DEFb5D67ee92e774f1841"),
        "BTC-SS",
    ),
    (
        &address!("0xC8fc7E7E7B4D94FA02751B8719F5BbBb4C1413Cf"),
        "ETH-SS",
    ),
    (
        &address!("0x91B2745d7acA9D64560cD1693b6fF96678FfC433"),
        "FOMO",
    ),
    (
        &address!("0x3B260979D18D4BE11A39B8C9CdD22f61E47BDEbc"),
        "ETH-S",
    ),
    (
        &address!("0x17840DF7CAa07e298b16E8612157B90ED231C973"),
        "DAO",
    ),
    (
        &address!("0x00D5149cDF7CEC8725bf50073c51c4fa58eCCa12"),
        "POWER",
    ),
    (
        &address!("0x0129258AeCD42b8A750FB13578A6E66B30b41a14"),
        "CORN",
    ),
    (
        &address!("0x012e89B2eBa5d449Ba6e5e3057F7112EC9C3FD86"),
        "PBAKE",
    ),
    (
        &address!("0x017CE65a6ac1726A69BeC4AfE0766e51C8d6465e"),
        "ROLL",
    ),
    (
        &address!("0x01bCEC6175FA9366D14b8275e95D0fA45702f696"),
        "GORILLA",
    ),
    (
        &address!("0x07150dCC5223CED5d3EF35D5D850FfA24Ad6be5B"),
        "BALLZ",
    ),
    (
        &address!("0x093b735dD3daEceDA74A4DC4352aF82EA54B806D"),
        "SHIBABY",
    ),
    (
        &address!("0x22734Bfa0bD1589BC283E78F672EdD5736503dfA"),
        "CRAB",
    ),
    (
        &address!("0x292F16a3D5Ad04fA2209aD0E65c58C4B9F7f16b8"),
        "LMS",
    ),
    (
        &address!("0x32934CB16DA43fd661116468c1B225Fc26CF9A8c"),
        "SNE",
    ),
    (
        &address!("0x6b140057F140e082B2501399915D9629751b011e"),
        "POLYTOKEN",
    ),
    (
        &address!("0x6cC2c94Bf853FcA7Ee473b2a7186D5251099697e"),
        "APPLE",
    ),
    (
        &address!("0x782eb3304F8b9adD877F13a5cA321f72c4AA9804"),
        "PLATIN",
    ),
    (
        &address!("0x7D41E0D59149F018D0D5B93F44B65f8ae0b90d6d"),
        "GOLD",
    ),
    (
        &address!("0x8b8E89fdB1Ec4696c604FCFEbeD2740E4c727900"),
        "KTEST",
    ),
    (
        &address!("0x91eE7D76599Fdc8da86FD291423139BF5D679CF0"),
        "LMOOSE",
    ),
    (
        &address!("0xBC6B630B014e941A921CDc0967AAea2a8aB932B5"),
        "PCP",
    ),
    (
        &address!("0xc2E93cC8E8EC96076D20f9d9d16f8d415D90CFb8"),
        "MiniDoge",
    ),
    (
        &address!("0xe4e8d9CfD123699a040219165E22Fa912D47147b"),
        "PONA",
    ),
    (
        &address!("0xE9f2e81894D34aefE7b6bBc89898fF92C39Db320"),
        "UTOPIA",
    ),
    (
        &address!("0xF501dd45a1198C2E1b5aEF5314A68B9006D842E0"),
        "MTA",
    ),
    (
        &address!("0xfd5962484BE2c3574D70131BF5D452CcC7C69F67"),
        "UCT",
    ),
    // --- runtime unmapped `?:` (2026-07-31 demand log) — on-chain symbol + name ---
    (
        // RAR / RecklessAndRelentless
        &address!("0x0259ddfBD48e65C22Ca365bc32AFF5B0a5fB9567"),
        "RAR",
    ),
    (
        // APE / ApeCoin (PoS) — Crypto.APE/USD in TOKEN_FEEDS
        &address!("0xB7b31a6BC18e48888545CE79e83E06003bE70930"),
        "APE",
    ),
    (
        // BTC-S / BTC Yield Index by StarSeeds
        &address!("0xa147901A0bB2B6DA6b9e10c69020285db7eCd0DF"),
        "BTC-S",
    ),
    (
        // EMI / Emicoin (PoS)
        &address!("0xFbeCfB7b87752aa28383a5Ac1e9d9e05E0526017"),
        "EMI",
    ),
    (
        // BRZ / BRZ Token (BRL-pegged)
        &address!("0x4eD141110F6EeeAbA9A1df36d8c26f684d2475Dc"),
        "BRZ",
    ),
    (
        // MLC / MyLovelyCoin
        &address!("0x0566C506477cD2d8dF4e0123512dBc344bD9D111"),
        "MLC",
    ),
    (
        // CES / WhaleBit
        &address!("0x1Bdf71EDe1a4777dB1EebE7232BcdA20d6FC1610"),
        "CES",
    ),
    (
        // GGT / GO GAME TOKEN
        &address!("0x8349314651eDe274f8c5FeF01Aa65fF8da75E57c"),
        "GGT",
    ),
    (
        // MEGA / MEGA STAKE TOKEN
        &address!("0x797444569d03052A171aAc4524591044264AbD2B"),
        "MEGA",
    ),
    (
        // DOMME / Domme
        &address!("0xf1CcF7f6aa6e5CF141dE54351E8E30A618945530"),
        "DOMME",
    ),
    (
        // KRW / KROWN (not Korean won)
        &address!("0x6c3B2f402CD7d22AE2C319B9d2f16f57927a4A17"),
        "KRW",
    ),
    (
        // SSG / STAR-GOV by StarSeeds
        &address!("0x8519d8DeaE7bA984C4C7850B11c4671b4f62b330"),
        "SSG",
    ),
    (
        // BAE / BaeBay
        &address!("0xbAe1b833cbA827BAFe783697A7d3D285a326233C"),
        "BAE",
    ),
    (
        // BULL / BullRun Token
        &address!("0xeDd4b24BE3c43De98989C233E67dB29Dc6554fC6"),
        "BULL",
    ),
    (
        // MCASH / Monsoon Finance
        &address!("0xa25610a77077390A75aD9072A084c5FbC7d43A0d"),
        "MCASH",
    ),
    (
        // BRAIN / BrainSwap
        &address!("0x5C6014246FC7911F4dB270aA3910F23EECD61720"),
        "BRAIN",
    ),
    (
        // polyBUNNY / Polygon BUNNY Token
        &address!("0x4C16f69302CcB511c5Fac682c7626B9eF0Dc126a"),
        "polyBUNNY",
    ),
    (
        // WORK / The Employment Commons Work Token
        &address!("0x6002410dDA2Fb88b4D0dc3c1D562F7761191eA80"),
        "WORK",
    ),
    (
        // PON / POLYGON Beplay
        &address!("0xA99cf31b7f04EE76C27BbD30C9770703D0F5C3af"),
        "PON",
    ),
    (
        // TRZ / PolyTreasure Token
        &address!("0x13436a3c5c2574C8145222260B0ed4C2Da31f760"),
        "TRZ",
    ),
    (
        // IMX / Impermax (PoS) — NOT Immutable X; do not map Crypto.IMX/USD
        &address!("0x60bB3D364B765C497C8cE50AE0Ae3f0882c5bD05"),
        "IMX",
    ),
    (
        &address!("0x6bb45cEAC714c52342Ef73ec663479da35934bf7"),
        "BONE",
    ),
    (
        &address!("0x8Ab2Fec94d17ae69FB90E7c773f2C85Ed1802c01"),
        "LQTY",
    ),
    (
        &address!("0xC467Dc2f2fA605ff590DbE56E7E71AbA90e15813"),
        "PPGv2",
    ),
    (
        &address!("0xEdEc94e7828135d8fDF426a72eF357Fe14cE9526"),
        "LOL",
    ),
    (
        &address!("0xaC3090B7042FCA2cDBF233022e4a9823a032600c"),
        "BETA",
    ),
    (
        &address!("0xF480f38C366dAaC4305dC484b2Ad7a496FF00CeA"),
        "GTON",
    ),
    (
        &address!("0x0B220b82F3eA3B7F6d9A1D8ab58930C064A2b5Bf"),
        "GLM",
    ),
    (
        // POLYWHORE / WHORE.FINANCE
        &address!("0x6C9f3c8EF26cd9e6479F579901a940856C8b3aa0"),
        "POLYWHORE",
    ),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_unmapped_contract_labels_are_present() {
        let labels = [
            ("0x00D5149cDF7CEC8725bf50073c51c4fa58eCCa12", "POWER"),
            ("0x0129258AeCD42b8A750FB13578A6E66B30b41a14", "CORN"),
            ("0x012e89B2eBa5d449Ba6e5e3057F7112EC9C3FD86", "PBAKE"),
            ("0x017CE65a6ac1726A69BeC4AfE0766e51C8d6465e", "ROLL"),
            ("0x01bCEC6175FA9366D14b8275e95D0fA45702f696", "GORILLA"),
            ("0x07150dCC5223CED5d3EF35D5D850FfA24Ad6be5B", "BALLZ"),
            ("0x093b735dD3daEceDA74A4DC4352aF82EA54B806D", "SHIBABY"),
            ("0x22734Bfa0bD1589BC283E78F672EdD5736503dfA", "CRAB"),
            ("0x292F16a3D5Ad04fA2209aD0E65c58C4B9F7f16b8", "LMS"),
            ("0x32934CB16DA43fd661116468c1B225Fc26CF9A8c", "SNE"),
            ("0x6b140057F140e082B2501399915D9629751b011e", "POLYTOKEN"),
            ("0x6cC2c94Bf853FcA7Ee473b2a7186D5251099697e", "APPLE"),
            ("0x782eb3304F8b9adD877F13a5cA321f72c4AA9804", "PLATIN"),
            ("0x7D41E0D59149F018D0D5B93F44B65f8ae0b90d6d", "GOLD"),
            ("0x8b8E89fdB1Ec4696c604FCFEbeD2740E4c727900", "KTEST"),
            ("0x91eE7D76599Fdc8da86FD291423139BF5D679CF0", "LMOOSE"),
            ("0xBC6B630B014e941A921CDc0967AAea2a8aB932B5", "PCP"),
            ("0xc2E93cC8E8EC96076D20f9d9d16f8d415D90CFb8", "MiniDoge"),
            ("0xe4e8d9CfD123699a040219165E22Fa912D47147b", "PONA"),
            ("0xE9f2e81894D34aefE7b6bBc89898fF92C39Db320", "UTOPIA"),
            ("0xF501dd45a1198C2E1b5aEF5314A68B9006D842E0", "MTA"),
            ("0xfd5962484BE2c3574D70131BF5D452CcC7C69F67", "UCT"),
            ("0xfB021f1Ce0d5327eE31EFE21F9b23c0b9A9ccC58", "fTIME"),
            ("0x1deFA61862E27E5C9E8b9f8daE769290B9CF6D93", "ELONINDEX"),
            ("0x35aC92580eb672d9c76FeE4228165F6eF224c334", "ELONINDEX"),
            ("0xB29781B8B09ba707631eE6720E50129777d4ba31", "PFC"),
            ("0x0E9b89007eEE9c958c0EDA24eF70723C2C93dD58", "ankrMATIC"),
            ("0x5118aeC3AfCca3f1e21733eE9C88BB800AFE6F7b", "FLDG"),
            ("0x86233fF3eD561A0E35910920acB2D91419Fd14d0", "DOGE"),
            ("0x9Cb74C8032b007466865f060ad2c46145d45553D", "IDEX"),
            ("0xa8c17604f6C9fFA20955b40CC3B607Bf9606b625", "DOGEINDEX"),
            ("0xEe9A352F6aAc4aF1A5B9f467F6a93E0ffBe9Dd35", "MASQ"),
            ("0x02753ee6a439BE76cFAF828CeBE02ce88eCFf8b5", "NOW"),
            ("0xe1398B5d2f3CEF77a13a7CcBae33F2121c217301", "RODO"),
            ("0x00e5646f60AC6Fb446f621d146B6E1886f002905", "RAI"),
            ("0x454551c6027941BA34e149CC92A8dfC30c191570", "EFP"),
            ("0x64c1B62Fe216B167BC7fd5B217630d494954b4c4", "ELONINDEX"),
            ("0x8031c44b96Ec8c9B66aB16c2c164e8dEEb361a3f", "LIRA"),
            ("0xB0ac8D198088cf310F45d9d05C747263be994849", "UCTL"),
            ("0x5a5d62660F5B0556d33513869a4C9bA97d2d88e4", "CTK multi"),
            ("0x62F594339830b90AE4C084aE7D223fFAFd9658A7", "SPHERE"),
            ("0xfe48ee5070a045B2Caa2Ce9e344735D9E4886B46", "3TK"),
            ("0x1c67201D5B748C9666205c2a1f7E2d4272F5Da3F", "ENTER"),
            // Fixed mislabels (on-chain symbol)
            ("0x72d31b6dD46DaaE07391036097A2CB4648991eCD", "SP"),
            ("0x4b0dF7EDe79be6b046a4Ed71580A3733A109e641", "USD-SS"),
            ("0x0d929e52EFBf26F2322fd4033B157538c3b80474", "KOR"),
            // Runtime `?:` batch 2026-07-31
            ("0x0259ddfBD48e65C22Ca365bc32AFF5B0a5fB9567", "RAR"),
            ("0xB7b31a6BC18e48888545CE79e83E06003bE70930", "APE"),
            ("0xa147901A0bB2B6DA6b9e10c69020285db7eCd0DF", "BTC-S"),
            ("0xFbeCfB7b87752aa28383a5Ac1e9d9e05E0526017", "EMI"),
            ("0x4eD141110F6EeeAbA9A1df36d8c26f684d2475Dc", "BRZ"),
            ("0x0566C506477cD2d8dF4e0123512dBc344bD9D111", "MLC"),
            ("0x1Bdf71EDe1a4777dB1EebE7232BcdA20d6FC1610", "CES"),
            ("0x8349314651eDe274f8c5FeF01Aa65fF8da75E57c", "GGT"),
            ("0x797444569d03052A171aAc4524591044264AbD2B", "MEGA"),
            ("0xf1CcF7f6aa6e5CF141dE54351E8E30A618945530", "DOMME"),
            ("0x6c3B2f402CD7d22AE2C319B9d2f16f57927a4A17", "KRW"),
            ("0x8519d8DeaE7bA984C4C7850B11c4671b4f62b330", "SSG"),
            ("0xbAe1b833cbA827BAFe783697A7d3D285a326233C", "BAE"),
            ("0xeDd4b24BE3c43De98989C233E67dB29Dc6554fC6", "BULL"),
            ("0xa25610a77077390A75aD9072A084c5FbC7d43A0d", "MCASH"),
            ("0x5C6014246FC7911F4dB270aA3910F23EECD61720", "BRAIN"),
            ("0x4C16f69302CcB511c5Fac682c7626B9eF0Dc126a", "polyBUNNY"),
            ("0x6002410dDA2Fb88b4D0dc3c1D562F7761191eA80", "WORK"),
            ("0xA99cf31b7f04EE76C27BbD30C9770703D0F5C3af", "PON"),
            ("0x13436a3c5c2574C8145222260B0ed4C2Da31f760", "TRZ"),
            ("0x60bB3D364B765C497C8cE50AE0Ae3f0882c5bD05", "IMX"),
            ("0x6bb45cEAC714c52342Ef73ec663479da35934bf7", "BONE"),
            ("0x8Ab2Fec94d17ae69FB90E7c773f2C85Ed1802c01", "LQTY"),
            ("0xC467Dc2f2fA605ff590DbE56E7E71AbA90e15813", "PPGv2"),
            ("0xEdEc94e7828135d8fDF426a72eF357Fe14cE9526", "LOL"),
            ("0xaC3090B7042FCA2cDBF233022e4a9823a032600c", "BETA"),
            ("0xF480f38C366dAaC4305dC484b2Ad7a496FF00CeA", "GTON"),
            ("0x0B220b82F3eA3B7F6d9A1D8ab58930C064A2b5Bf", "GLM"),
            ("0x6C9f3c8EF26cd9e6479F579901a940856C8b3aa0", "POLYWHORE"),
        ];
        for (address, symbol) in labels {
            let address = address.parse().expect("valid test address");
            assert_eq!(lookup_symbol(&address), Some(symbol));
        }
    }
}
