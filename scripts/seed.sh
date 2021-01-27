#! /bin/bash
db="demo"

if [ "$1" != "" ]; then
    db=$1
fi

curl -H 'Content-Type: application/json' \
    -X DELETE http://127.0.0.1:5984/$db
curl -H 'Content-Type: application/json' \
    -X PUT http://127.0.0.1:5984/$db

blah='"Header": {
    "SchemaVersion": "2.1",
    "DocumentType": "MC",
    "SourceSystemIdentifier": "CDI",
    "SubSystemIdentifier": "Primary",
    "DocumentSequenceNumber": 1100859773,
    "SourceSystemPrimaryKey": "3001501463",
    "JobIdentifier": "0",
    "EventDateTime": "2020-08-26 14:46:38.480699",
    "EventType": "CHANGE",
    "BuildVersion": "1.00.01"
  },
  "Body": {
    "Party": {
      "PartyIdentifier": 3001501463,
      "PartyPrimaryKey_CDI": "1035900       ",
      "FirstName": "BRENDAN",
      "LastName": "BAILEY",
      "FullName": "BRENDAN BAILEY",
      "TaxIdentifierToken": "A1X2X0000",
      "TaxIDTypeCode": "SSN",
      "TaxIDValidIndicator": "Y",
      "DeceasedIndicator": "N",
      "BirthDate": "1980-03-02",
      "PartyStatus": "A",
      "PartySoftDeleteIndicator": "N",
      "PartyTypeCode": "PRSN",
      "GenderCode": "UNK",
      "DriverLicenseStateCode": "UNK",
      "CDIPartyClass": "C",
      "PartyConsolidationDesc": "Consolidated",
      "MixedBrandIndicator": "Y",
      "MixedBrandCAIndicator": "N",
      "PersonList": [
        {
          "PersonPrimaryKey_CDI": "4291498       ",
          "VIPIndicator": "N",
          "ChildCustodyIndicator": "N",
          "CriminalConvictionIndicator": "N",
          "HighestEducationCode": "BCHL",
          "PersonSoftDeleteIndicator": "N",
          "PersonLastUpdateDate": "2020-08-26 14:42:39",
          "MaritalStatusCode": "SNGL",
          "LanguageCode": "ENG",
          "ContactMethodCode": "UNK"
        },
        {
          "PersonPrimaryKey_CDI": "1113591       ",
          "VIPIndicator": "N",
          "ChildCustodyIndicator": "N",
          "CriminalConvictionIndicator": "N",
          "HighestEducationCode": "MSTR",
          "PersonSoftDeleteIndicator": "N",
          "PersonLastUpdateDate": "2019-09-10 14:07:24",
          "MaritalStatusCode": "SNGL",
          "LanguageCode": "ENG",
          "ContactMethodCode": "UNK"
        }
      ],
      "PartyContractList": [
        {
          "ContractPrimaryKey_CDI": "3897312       ",
          "ContractID": 99883111777777,
          "PolicyNumber": "H3CS1210498888",
          "PolicyEffectiveDate": "2017-11-09",
          "PolicyExpirationDate": "2018-11-09",
          "BillingInfoIdentifier": "998891122222222",
          "ContractNumber": "H3CS1210496880",
          "QuoteIdentifier": "99883111777777",
          "QuoteEffectiveDate": "2017-11-08",
          "ContractTypeCode": "POL",
          "ProductTypeCode": "PROPERTY",
          "ContractStatusCode": "I",
          "PolicyStateCode": "ME",
          "ContractSoftDeleteIndicator": "N",
          "WebQuoteIdentifier": "03776728",
          "ReferralSourceCode": "024",
          "AgencyNumber": "11680000",
          "BrandCode": "SAF",
          "ProductLineCode": "HM",
          "ContractSourceSystemCode": "CF",
          "PartyContractPrimaryKey_CDI": "6226497       ",
          "PCStatus": "A",
          "PCSoftDeleteIndicator": "N",
          "PCLastUpdateDate": "2019-09-05 22:42:58",
          "PartyContractRoleList": [
            {
              "PartyContractRoleTypeCode": "NI",
              "PCRStatus": "A",
              "PCRSoftDeleteIndicator": "N"
            }
          ]
        },
        {
          "ContractPrimaryKey_CDI": "3922879       ",
          "ContractID": 998931122142301,
          "PolicyEffectiveDate": "2017-11-09",
          "PolicyExpirationDate": "2018-11-09",
          "BillingInfoIdentifier": "998891122263433",
          "ContractNumber": "998831117987075",
          "QuoteIdentifier": "998831117987075",
          "QuoteEffectiveDate": "2017-11-08",
          "ContractTypeCode": "QTE",
          "ProductTypeCode": "PROPERTY",
          "ContractStatusCode": "D",
          "PolicyStateCode": "ME",
          "ContractSoftDeleteIndicator": "N",
          "WebQuoteIdentifier": "03776728",
          "ReferralSourceCode": "024",
          "AgencyNumber": "11680000",
          "BrandCode": "SAF",
          "ProductLineCode": "HM",
          "ContractSourceSystemCode": "CF",
          "PartyContractPrimaryKey_CDI": "6249167       ",
          "PCStatus": "A",
          "PCSoftDeleteIndicator": "N",
          "PCLastUpdateDate": "2019-09-05 22:42:59",
          "PartyContractRoleList": [
            {
              "PartyContractRoleTypeCode": "NI",
              "PCRStatus": "A",
              "PCRSoftDeleteIndicator": "N"
            }
          ]
        },
        {
          "ContractPrimaryKey_CDI": "3922882       ",
          "ContractID": 998931122142534,
          "PolicyEffectiveDate": "2017-11-09",
          "PolicyExpirationDate": "2018-11-09",
          "BillingInfoIdentifier": "998891122263437",
          "ContractNumber": "998831117987075",
          "QuoteIdentifier": "998831117987075",
          "QuoteEffectiveDate": "2017-11-08",
          "ContractTypeCode": "QTE",
          "ProductTypeCode": "PROPERTY",
          "ContractStatusCode": "D",
          "PolicyStateCode": "ME",
          "ContractSoftDeleteIndicator": "N",
          "WebQuoteIdentifier": "03776728",
          "ReferralSourceCode": "024",
          "AgencyNumber": "11680000",
          "BrandCode": "SAF",
          "ProductLineCode": "HM",
          "ContractSourceSystemCode": "CF",
          "PartyContractPrimaryKey_CDI": "6249169       ",
          "PCStatus": "A",
          "PCSoftDeleteIndicator": "N",
          "PCLastUpdateDate": "2019-09-05 22:42:59",
          "PartyContractRoleList": [
            {
              "PartyContractRoleTypeCode": "NI",
              "PCRStatus": "A",
              "PCRSoftDeleteIndicator": "N"
            }
          ]
        },
        {
          "ContractPrimaryKey_CDI": "3897313       ",
          "ContractID": 998831117987108,
          "PolicyNumber": "H3CS1210496880",
          "PolicyEffectiveDate": "2017-11-09",
          "PolicyExpirationDate": "2018-11-09",
          "BillingInfoIdentifier": "998871118613343",
          "ContractNumber": "H3CS1210496880",
          "QuoteIdentifier": "998831117987075",
          "QuoteEffectiveDate": "2017-11-08",
          "ContractTypeCode": "POL",
          "ProductTypeCode": "PROPERTY",
          "ContractStatusCode": "A",
          "PolicyStateCode": "ME",
          "ContractSoftDeleteIndicator": "N",
          "WebQuoteIdentifier": "03776728",
          "ReferralSourceCode": "024",
          "AgencyNumber": "11680000",
          "BrandCode": "SAF",
          "ProductLineCode": "HM",
          "ContractSourceSystemCode": "CF",
          "PartyContractPrimaryKey_CDI": "6226499       ",
          "PCStatus": "A",
          "PCSoftDeleteIndicator": "N",
          "PCLastUpdateDate": "2019-09-05 22:42:58",
          "PartyContractRoleList": [
            {
              "PartyContractRoleTypeCode": "NI",
              "PCRStatus": "A",
              "PCRSoftDeleteIndicator": "N"
            }
          ]
        },
        {
          "ContractPrimaryKey_CDI": "2479085       ",
          "PolicyEffectiveDate": "2017-08-20",
          "ContractNumber": "c9c583fb-daee-452e-b34c-0ce8d73adbb5",
          "QuoteEffectiveDate": "2017-08-20",
          "ContractTypeCode": "QTE",
          "ProductTypeCode": "AUTO",
          "ContractStatusCode": "A",
          "PolicyStateCode": "IL",
          "ContractSoftDeleteIndicator": "N",
          "AgencyNumber": "24282178",
          "BrandCode": "SAF",
          "ProductLineCode": "AUT",
          "ContractSourceSystemCode": "SQQ",
          "PartyContractPrimaryKey_CDI": "3767358       ",
          "PCStatus": "A",
          "PCSoftDeleteIndicator": "N",
          "PCLastUpdateDate": "2019-09-05 22:27:14",
          "PartyContractRoleList": [
            {
              "PartyContractRoleTypeCode": "NI",
              "PCRStatus": "A",
              "PCRSoftDeleteIndicator": "N"
            },
            {
              "PartyContractRoleTypeCode": "OP",
              "PCRStatus": "A",
              "PCRSoftDeleteIndicator": "N"
            }
          ]
        },
        {
          "ContractPrimaryKey_CDI": "3909651       ",
          "ContractID": 998881120170168,
          "PolicyNumber": "H3CS1210496880",
          "PolicyEffectiveDate": "2017-11-09",
          "PolicyExpirationDate": "2018-11-09",
          "BillingInfoIdentifier": "998891122263447",
          "ContractNumber": "998831117987075",
          "QuoteIdentifier": "998831117987075",
          "QuoteEffectiveDate": "2017-11-08",
          "ContractTypeCode": "QTE",
          "ProductTypeCode": "PROPERTY",
          "ContractStatusCode": "D",
          "PolicyStateCode": "ME",
          "ContractSoftDeleteIndicator": "N",
          "WebQuoteIdentifier": "03776728",
          "ReferralSourceCode": "024",
          "AgencyNumber": "11680000",
          "BrandCode": "SAF",
          "ProductLineCode": "HM",
          "ContractSourceSystemCode": "CF",
          "PartyContractPrimaryKey_CDI": "6238034       ",
          "PCStatus": "A",
          "PCSoftDeleteIndicator": "N",
          "PCLastUpdateDate": "2019-09-05 22:42:59",
          "PartyContractRoleList": [
            {
              "PartyContractRoleTypeCode": "NI",
              "PCRStatus": "A",
              "PCRSoftDeleteIndicator": "N"
            }
          ]
        },
        {
          "ContractPrimaryKey_CDI": "8752343       ",
          "PolicyEffectiveDate": "2020-10-24",
          "PolicyExpirationDate": "2021-10-24",
          "QuoteEffectiveDate": "2020-10-23",
          "ContractTypeCode": "QTE",
          "ProductTypeCode": "AUTO",
          "ContractStatusCode": "I",
          "PolicyStateCode": "IL",
          "ContractSoftDeleteIndicator": "N",
          "WebQuoteIdentifier": "Q20-08261-00644",
          "ReferralSourceCode": "007",
          "BrandCode": "LM",
          "ProductLineCode": "AUT",
          "ContractSourceSystemCode": "HLS",
          "PartyContractPrimaryKey_CDI": "13973928      ",
          "PCStatus": "A",
          "PCSoftDeleteIndicator": "N",
          "PCLastUpdateDate": "2020-08-26 14:40:09",
          "PartyContractRoleList": [
            {
              "PartyContractRoleTypeCode": "NI",
              "PCRStatus": "A",
              "PCRSoftDeleteIndicator": "N"
            }
          ]
        }
      ],
      "PartyAddressList": [
        {
          "AddressPrimaryKey_CDI": "100010        ",
          "StreetNumber": "303",
          "StreetName": "KIRBY",
          "StreetTypeCode": "AVE",
          "CityName": "CHAMPAIGN",
          "StateCode": "IL",
          "PostalCode": "61820",
          "PostalExtensionCode": "7207",
          "CountryCode": "USA",
          "GeoAccuracyCode": "2",
          "CensusTract": "001301",
          "TrilliumReturnCode": "0",
          "AddressLine1": "303 W KIRBY AVE",
          "IsValidIndicator": "Y",
          "StreetPreDirectionCode": "W",
          "Latitude": "+40098080",
          "Longitude": "-088247644",
          "PASoftDeleteIndicator": "N",
          "DoNotContactIndicator": "N",
          "AddressTypeCode": "RSID",
          "AddressTypePreferenceCode": "PRI",
          "PALastUpdateDate": "2019-09-05 22:26:29",
          "HouseholdID": "87826",
          "HouseholdIDLastUpdateDate": "2017-08-20"
        },
        {
          "AddressPrimaryKey_CDI": "100010        ",
          "StreetNumber": "303",
          "StreetName": "KIRBY",
          "StreetTypeCode": "AVE",
          "CityName": "CHAMPAIGN",
          "StateCode": "IL",
          "PostalCode": "61820",
          "PostalExtensionCode": "7207",
          "CountryCode": "USA",
          "GeoAccuracyCode": "2",
          "CensusTract": "001301",
          "TrilliumReturnCode": "0",
          "AddressLine1": "303 W KIRBY AVE",
          "IsValidIndicator": "Y",
          "StreetPreDirectionCode": "W",
          "Latitude": "+40098080",
          "Longitude": "-088247644",
          "PASoftDeleteIndicator": "N",
          "DoNotContactIndicator": "N",
          "AddressTypeCode": "RSID",
          "AddressTypePreferenceCode": "PRI",
          "PALastUpdateDate": "2020-08-26 14:40:09",
          "HouseholdID": "87826",
          "HouseholdIDLastUpdateDate": "2017-08-20"
        },
        {
          "AddressPrimaryKey_CDI": "11718         ",
          "CityName": "RESIDENTIAL CITY",
          "StateCode": "ME",
          "PostalCode": "04736",
          "CountryCode": "USA",
          "TrilliumReturnCode": "2",
          "AddressLine1": "100 RESIDENTIAL RD",
          "IsValidIndicator": "N",
          "PASoftDeleteIndicator": "N",
          "DoNotContactIndicator": "N",
          "AddressTypeCode": "RSID",
          "AddressTypePreferenceCode": "PRI",
          "PALastUpdateDate": "2019-09-05 22:41:22",
          "HouseholdID": "220313",
          "HouseholdIDLastUpdateDate": "2017-11-08"
        },
        {
          "AddressPrimaryKey_CDI": "164596        ",
          "StreetNumber": "0",
          "StreetName": "MAIN",
          "StreetTypeCode": "ST",
          "CityName": "LEWISTON",
          "StateCode": "ME",
          "PostalCode": "04240",
          "PostalExtensionCode": "6466",
          "CountryCode": "USA",
          "GeoAccuracyCode": "2",
          "CensusTract": "020400",
          "TrilliumReturnCode": "0",
          "AddressLine1": "0 MAIN ST",
          "IsValidIndicator": "Y",
          "Latitude": "+44098167",
          "Longitude": "-070202084",
          "PASoftDeleteIndicator": "Y",
          "PASoftDeleteDate": "2017-11-08 18:01:53",
          "DoNotContactIndicator": "N",
          "AddressTypeCode": "RSID",
          "AddressTypePreferenceCode": "PRI",
          "PALastUpdateDate": "2019-09-05 22:41:22",
          "HouseholdID": "217416",
          "HouseholdIDLastUpdateDate": "2017-11-08"
        }
      ],
      "PartyElecAddrList": [
        {
          "EAPrimaryKey_CDI": "126270        ",
          "ElectronicAddress": "KATHLEEN.BAILEY465@EXAMPLE.COM",
          "IsValidIndicator": "Y",
          "PEASoftDeleteIndicator": "N",
          "DoNotContactIndicator": "N",
          "ElectronicAddressTypeCode": "EMAIL",
          "PEALastUpdateDate": "2020-08-26 14:40:09"
        }
      ],
      "PartyPhoneList": [
        {
          "PhonePrimaryKey_CDI": "24179         ",
          "PhoneNumber": "2075838573",
          "IsValidIndicator": "Y",
          "PartyPhoneSoftDeleteIndicator": "N",
          "DoNotContactIndicator": "N",
          "PhoneTypeCode": "RES",
          "PPLastUpdateDate": "2020-08-26 14:42:22"
        }
      ],
      "PartyCrossRefList": [
        {
          "PartyXrefPkeySrcObject": "c9c583fb-daee-452e-b34c-0ce8d73adbb5|OP|001",
          "PartyXrefRowidXref_CDI": 1037501,
          "PartyXrefPrimaryKey_CDI": "1035900       ",
          "SourceSystemCode": "SQQ",
          "SourceLastUpdateDate": "2017-08-20 07:00:00",
          "SourcePartyIdentifier": 3001501463,
          "OrganizationID": "2000012559861",
          "SourceRowidObject_CDI": "1035900       ",
          "TargetRowidObject_CDI": "1035900       ",
          "MatchRuleRowidObject_CDI": "SVR1.1I79H2   ",
          "MergeDate": "2017-11-08 20:03:37",
          "SourcePrimaryKey": "c9c583fb-daee-452e-b34c-0ce8d73adbb5|OP|001",
          "PartyXrefLastUpdateDate": "2019-09-05 21:19:51"
        },
        {
          "PartyXrefPkeySrcObject": "5f467446d9915500487b0f69",
          "PartyXrefRowidXref_CDI": 5311956,
          "PartyXrefPrimaryKey_CDI": "1035900       ",
          "SourceSystemCode": "HLS",
          "SourceLastUpdateDate": "2020-08-26 14:42:20",
          "SourcePartyIdentifier": 3005593210,
          "SourceRowidObject_CDI": "1035900       ",
          "TargetRowidObject_CDI": "1035900       ",
          "MatchRuleRowidObject_CDI": "SVR1.5Y1N     ",
          "MergeDate": "2020-08-26 14:40:17",
          "SourcePrimaryKey": "5f467446d9915500487b0f69",
          "PartyXrefLastUpdateDate": "2020-08-26 14:42:22",
          "SourcePartyStatus": "ACTIVE"
        },
        {
          "PartyXrefPkeySrcObject": "998411123683963",
          "PartyXrefRowidXref_CDI": 1373932,
          "PartyXrefPrimaryKey_CDI": "1035900       ",
          "SourceSystemCode": "CF",
          "SourceLastUpdateDate": "2017-11-08 18:01:53",
          "SourcePartyIdentifier": 3001645923,
          "VisionHouseholdID": 998411123683962,
          "OrganizationID": "2000012559861",
          "SourcePrimaryKey": "998411123683963",
          "PartyXrefLastUpdateDate": "2019-09-05 21:40:59"
        }
      ]
    }
  }
'
id=10001
for i in {1..50}
do
    echo "seeding batch ${i}/50"
    for j in {1001..1250}
    do
        value=$(expr $i + $j)
        str=$(date)
        mesg='{ "value": '"${value}"', "ts": "'"${str}"'" '
        mesg=${mesg}','"${blah}"'}'
        curl -X PUT http://127.0.0.1:5984/$db/"${id}" -d "${mesg}" > /dev/null 2>&1
        id=$(expr $id + 1)
    done
done