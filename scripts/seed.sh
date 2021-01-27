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
      "MixedBrandCAIndicator": "N"
    }
  }
'
id=10001
for i in {1001..1250}
do
    for j in {1..50}
    do
        value=$(expr $i + $j)
        str=$(date)
        mesg='{ "value": '"${value}"', "ts": "'"${str}"'" '
        mesg=${mesg}','"${blah}"'}'
        curl -X PUT http://127.0.0.1:5984/$db/"${id}" -d "${mesg}"
        id=$(expr $id + 1)
    done
done