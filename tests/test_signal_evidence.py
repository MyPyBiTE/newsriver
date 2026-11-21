import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch
from importlib.util import spec_from_file_location,module_from_spec

SCRIPT=Path(__file__).resolve().parents[1]/'scripts'/'refresh_signal_evidence.py'
spec=spec_from_file_location('refresh_signal_evidence',SCRIPT)
m=module_from_spec(spec)
spec.loader.exec_module(m)


def sec_fixture():
    data=[[320193,'Apple Inc.','AAPL','Nasdaq'],
          [789019,'MICROSOFT CORP','MSFT','Nasdaq'],
          [1067983,'BERKSHIRE HATHAWAY INC','BRK-B','NYSE'],
          [2130357,'Anthropic, PBC','ANT',None]]
    data += [[3000000+n, f'Example {n}','X'+str(n), 'NYSE'] for n in range(1100)]
    return {'data':data}


def finra_fixture(asof=None):
    asof=asof or (date.today()-timedelta(days=14)).isoformat()
    return {'schema':m.FINRA_SCHEMA,'settlementDate':asof,
            'data':[{'ticker':'AAPL','cik':'0000320193','shortShares':10000000,'settlementDate':asof},
                    {'ticker':'BRK.B','cik':'0001067983','shortShares':250000,'settlementDate':asof}]}


def massive_fixture(asof=None):
    asof=asof or (date.today()-timedelta(days=20)).isoformat()
    return {'status':'OK','results':[
        {'ticker':'AAPL','free_float':1000000000,'effective_date':asof},
        {'ticker':'BRK.B','free_float':10000000,'effective_date':asof}]}


class SignalEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.directory=m.directory_from_sec(sec_fixture())

    def assessment(self, shorts=None,floats=None):
        return m.build_assessment(self.directory,shorts or finra_fixture(),floats or massive_fixture())

    def ticker(self,result,ticker):
        return next(row for row in result['data'] if row['ticker']==ticker)

    def test_sec_2026_anthropic_identity(self):
        row=next(row for row in self.directory['data'] if row[0]=='ANT')
        self.assertEqual(row[2],'0002130357')
        self.assertEqual(row[1],'Anthropic, PBC')

    def test_apple_is_scored_with_verified_dated_float(self):
        row=self.ticker(self.assessment(),'AAPL')
        self.assertEqual((row['status'],row['points'],row['shortPct']),('scored',10,1.0))
        self.assertEqual(row['shortShares'],10000000)

    def test_anthropic_has_no_pre_ipo_finra_record(self):
        row=self.ticker(self.assessment(),'ANT')
        self.assertEqual(row['status'],'no-finra-record')
        self.assertIsNone(row['points'])

    def test_berkshire_class_separator_normalizes_without_merging_classes(self):
        row=self.ticker(self.assessment(),'BRK-B')
        self.assertEqual((row['shortShares'],row['shortPct'],row['points']),(250000,2.5,8))
        self.assertNotEqual(m.canonical_symbol('BRK.A'),m.canonical_symbol('BRK.B'))

    def test_finra_cik_mismatch_never_scores(self):
        src=finra_fixture()
        src['data'][0]['cik']='0000789019'
        row=self.ticker(self.assessment(src),'AAPL')
        self.assertEqual(row['status'],'no-finra-record')
        self.assertIsNone(row['points'])

    def test_float_after_settlement_never_scores(self):
        later=(date.today()+timedelta(days=3)).isoformat()
        src=massive_fixture()
        src['results'][0]['effective_date']=later
        row=self.ticker(self.assessment(floats=src),'AAPL')
        self.assertEqual(row['status'],'float-after-settlement')
        self.assertIsNone(row['shortPct'])

    def test_stale_float_never_scores(self):
        src=massive_fixture()
        src['results'][0]['effective_date']=(date.today()-timedelta(days=180)).isoformat()
        row=self.ticker(self.assessment(floats=src),'AAPL')
        self.assertEqual(row['status'],'stale-free-float')

    def test_sec_recent_filings_are_exact_cik_and_link_originals(self):
        fixture={'cik':'2130357','filings':{'recent':{
          'accessionNumber':['0001628280-26-074944','0001628280-26-074338'],
          'form':['424B4','S-1/A'],
          'filingDate':['2026-11-19','2026-11-17'],
          'primaryDocument':['anthropicpbc-424b4.htm','anthropicpbc-sx1ax3.htm']}}}
        rows=m.recent_filings_for_issuer(fixture,'ANT','0002130357','Anthropic, PBC')
        self.assertEqual(len(rows),2)
        self.assertTrue(all(item['cik']=='0002130357' for item in rows))
        self.assertTrue(rows[0]['url'].startswith('https://www.sec.gov/Archives/edgar/data/2130357/'))
        with self.assertRaisesRegex(ValueError,'mismatch'):
            m.recent_filings_for_issuer(fixture,'ANTA','0000001000','Unknown')

    def test_threshold_boundaries(self):
        self.assertEqual([m.short_score(x) for x in (0,2,2.01,5,5.01,10,10.01,20,20.01)],
                         [10,10,8,8,5,5,2,2,0])

    def test_reject_duplicate_sec_tickers_with_different_ciks(self):
        src=sec_fixture()
        src['data'].append([9999999,'Imposter','ANT','Nasdaq'])
        with self.assertRaisesRegex(ValueError,'ambiguous'):
            m.directory_from_sec(src)

    def test_no_live_api_needed_for_end_to_end_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)
            for name,obj in [('sec.json',sec_fixture()),('finra.json',finra_fixture()),('float.json',massive_fixture())]:
                (src/name).write_text(json.dumps(obj),encoding='utf-8')
            directory=src/'dir.json'; result=src/'out.json'
            status=m.main(['--sec-json',str(src/'sec.json'),'--finra-json',str(src/'finra.json'),
                           '--float-json',str(src/'float.json'),'--directory-out',str(directory),
                           '--assessment-out',str(result),'--skip-sec-submissions'])
            self.assertEqual(status,0)
            self.assertTrue(directory.is_file())
            self.assertEqual(self.ticker(json.loads(result.read_text()),'AAPL')['points'],10)

    def test_error_does_not_overwrite_existing_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)
            (src/'sec.json').write_text(json.dumps(sec_fixture()),encoding='utf-8')
            (src/'finra.json').write_text(json.dumps(finra_fixture()),encoding='utf-8')
            (src/'float.json').write_text(json.dumps({'results':[]}),encoding='utf-8')
            result=src/'out.json'; result.write_text('ORIGINAL')
            with patch('sys.stderr'):
                status=m.main(['--sec-json',str(src/'sec.json'),'--finra-json',str(src/'finra.json'),
                               '--float-json',str(src/'float.json'),'--directory-out',str(src/'dir.json'),
                               '--assessment-out',str(result),'--skip-sec-submissions'])
            self.assertEqual(status,1)
            self.assertEqual(result.read_text(),'ORIGINAL')

if __name__=='__main__':
    unittest.main()
