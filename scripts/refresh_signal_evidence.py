#!/usr/bin/env python3
"""Refresh the SEC issuer directory and combine FINRA short shares with Massive free float.

The script reads the existing public FINRA display feed; it does not request FINRA
credentials and never prints or writes the Massive credential. Failed validation
leaves existing files unchanged (the GitHub Action commits only on full success).
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
from urllib.request import Request, urlopen

SEC_DIRECTORY_URL = 'https://www.sec.gov/files/company_tickers_exchange.json'
MASSIVE_FLOAT_URL = 'https://api.massive.com/stocks/vX/float'
FINRA_SCHEMA = 'mypybite.finra-display.v2'
SEC_SCHEMA = 'mypybite.ticker-directory.v1'
OUTPUT_SCHEMA = 'mypybite.finra-short-interest-assessment.v1'
# Policy: low short interest earns more of the 10 risk points. Review periodically.
THRESHOLDS = ((2, 10), (5, 8), (10, 5), (20, 2), (float('inf'), 0))
MAX_FLOAT_AGE_DAYS = 120


def canonical_symbol(s):
    """Normalize exchange notation without merging class A and class B."""
    return re.sub(r'[.]', '-', str(s or '').strip().upper())


def cik10(cik):
    try:
        number = int(str(cik).strip())
        return f'{number:010d}' if 0 < number <= 9_999_999_999 else ''
    except (ValueError, TypeError):
        return ''


def strict_date(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def fetch_json(url, headers=None, timeout=40):
    request = Request(url, headers={'Accept': 'application/json',
                                    'User-Agent': 'MYPYBITE Research contact via GitHub MyPyBiTE/newsriver',
                                    **(headers or {})})
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except HTTPError as exc:
        endpoint = urlsplit(url)
        raise RuntimeError(f'{endpoint.hostname}{endpoint.path} returned HTTP {exc.code}') from None


def directory_from_sec(sec):
    """Produce the existing client contract from official SEC data only."""
    raw = sec.get('data', [])
    if not isinstance(raw, list) or len(raw) < 1000:
        raise ValueError('SEC company list is missing or unexpectedly small')
    items, seen = [], {}
    for row in raw:
        if not isinstance(row, list) or len(row) < 4:
            continue
        cik, name, symbol, exchange = row[:4]
        ticker = canonical_symbol(symbol)
        code = cik10(cik)
        if not code or not ticker or not name or not re.fullmatch(r'[A-Z0-9][A-Z0-9-]{0,19}', ticker):
            continue
        if ticker in seen:
            if seen[ticker] != code:
                raise ValueError(f'SEC returned ambiguous ticker identity: {ticker}')
            continue
        seen[ticker] = code
        items.append([ticker, str(name).strip(), code, str(exchange or ''), 'US',
                      'sec-companyfacts', 'SEC filings can be checked automatically'])
    if len(items) < 1000:
        raise ValueError('SEC directory has insufficient validated identities')
    expected = {'AAPL': '0000320193', 'MSFT': '0000789019', 'BRK-B': '0001067983', 'ANT': '0002130357'}
    for ticker, code in expected.items():
        if seen.get(ticker) != code:
            raise ValueError(f'Official SEC ticker / CIK mismatch or missing: {ticker}')
    return {
        'schema': SEC_SCHEMA,
        'generatedAt': datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z'),
        'source': {'name': 'U.S. Securities and Exchange Commission', 'url': SEC_DIRECTORY_URL,
                   'description': 'Official issuer tickers, names, CIKs and exchanges.'},
        'fields': ['symbol','name','cik','exchange','country','fundamentalCoverage','coverageLabel'],
        'data': sorted(items, key=lambda x: (x[0], x[2]))
    }


def parse_finra(payload):
    if payload.get('schema') != FINRA_SCHEMA or not isinstance(payload.get('data'), list):
        raise ValueError('FINRA feed schema is not supported')
    settlement = strict_date(payload.get('settlementDate'))
    if settlement is None or settlement > date.today():
        raise ValueError('Invalid FINRA settlement date')
    if (date.today() - settlement).days > 120:
        raise ValueError('FINRA feed too stale to publish assessment')
    by_key = {}
    for item in payload['data']:
        ticker = canonical_symbol(item.get('ticker'))
        cik = cik10(item.get('cik'))
        shares = item.get('shortShares')
        if (not ticker or not cik or type(shares) is not int or shares < 0 or
                shares > 2**53 - 1 or strict_date(item.get('settlementDate')) != settlement):
            raise ValueError('FINRA item malformed')
        key=(ticker,cik)
        if key in by_key:
            raise ValueError(f'Duplicate FINRA issuer/security: {ticker} {cik}')
        by_key[key]=item
    return settlement, by_key


def parse_float(payload):
    if payload.get('status') != 'OK' or not isinstance(payload.get('results'), list):
        raise ValueError('Massive float response unexpected')
    items = {}
    for row in payload['results']:
        ticker=canonical_symbol(row.get('ticker'))
        count=row.get('free_float')
        effective=strict_date(row.get('effective_date'))
        if not ticker or type(count) is not int or count <= 0 or not effective:
            continue
        prev=items.get(ticker)
        if prev and prev['effective_date'] > effective:
            continue
        if prev and prev['effective_date'] == effective and prev['free_float'] != count:
            raise ValueError(f'Conflicting float records for {ticker}')
        items[ticker]={'free_float': count, 'effective_date': effective}
    return items


def retrieve_massive_float(api_key, getter=fetch_json):
    if not api_key:
        raise ValueError('MASSIVE_API_KEY environment variable is required')
    results=[]
    next_url=MASSIVE_FLOAT_URL+'?'+urlencode({'limit':5000,'sort':'ticker.asc'})
    visited=set()
    for _ in range(10):
        if next_url in visited or not next_url.startswith(MASSIVE_FLOAT_URL):
            raise ValueError('Unsafe or repeated Massive pagination URL')
        visited.add(next_url)
        parts=urlsplit(next_url)
        # Never print the URL, which contains a secret query parameter.
        query=dict(parse_qsl(parts.query, keep_blank_values=True))
        query['apiKey']=api_key
        authorized_url=urlunsplit((parts.scheme,parts.netloc,parts.path,urlencode(query),''))
        data=getter(authorized_url)
        if data.get('status') != 'OK' or not isinstance(data.get('results'),list):
            raise ValueError('Massive float request failed or returned unsupported format')
        results.extend(data['results'])
        next_url=data.get('next_url')
        if not next_url:
            return {'status':'OK','results':results}
        if not isinstance(next_url,str):
            raise ValueError('Unexpected Massive next_url')
    raise ValueError('Massive pagination exceeded the safety limit')


def short_score(pct):
    if not 0 <= pct < float('inf'):
        raise ValueError('Invalid short interest percentage')
    return next(points for ceiling, points in THRESHOLDS if pct <= ceiling)


def build_assessment(directory, finra, float_payload):
    settlement, short_by_key=parse_finra(finra)
    floats=parse_float(float_payload)
    fields={field:i for i,field in enumerate(directory['fields'])}
    records=[]
    status_counts={}
    for row in directory['data']:
        ticker, cik = canonical_symbol(row[fields['symbol']]), cik10(row[fields['cik']])
        short=short_by_key.get((ticker,cik))
        matched_float=floats.get(ticker)
        status='scored'
        reason='Verified date-aligned FINRA shares short and Massive free float.'
        if not short:
            status='no-finra-record'; reason='No FINRA short-share record matched this exact ticker and CIK for this settlement.'
        elif matched_float is None:
            status='no-free-float'; reason='FINRA shares are available, but no matching Massive free-float record is available.'
        elif matched_float['effective_date'] > settlement:
            status='float-after-settlement'; reason='Latest Massive float measurement is newer than FINRA settlement; it cannot be used retroactively.'
        elif (settlement-matched_float['effective_date']).days > MAX_FLOAT_AGE_DAYS:
            status='stale-free-float'; reason=f'Massive free-float effective date is more than {MAX_FLOAT_AGE_DAYS} days before FINRA settlement.'
        short_count=short.get('shortShares') if short else None
        float_count=matched_float['free_float'] if matched_float else None
        effective=matched_float['effective_date'].isoformat() if matched_float else None
        raw_percentage=(short_count/float_count*100) if status=='scored' else None
        percentage=round(raw_percentage,4) if status=='scored' else None
        points=short_score(raw_percentage) if status=='scored' else None
        out={
            'ticker':ticker, 'cik':cik,
            'settlementDate':settlement.isoformat(),
            'shortShares':short_count,
            'freeFloatShares':float_count,
            'freeFloatEffectiveDate':effective,
            'shortPct':percentage,
            'points':points,
            'maxPoints':10,
            'status':status,
            'reason':reason
        }
        records.append(out)
        status_counts[status]=status_counts.get(status,0)+1
    return {
        'schema': OUTPUT_SCHEMA,
        'generatedAt': datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z'),
        'settlementDate':settlement.isoformat(),
        'policy':{'maxFloatAgeDays':MAX_FLOAT_AGE_DAYS,
                  'thresholds':[{'maxPct': ceiling if ceiling != float('inf') else None,'points':points}
                                for ceiling,points in THRESHOLDS],
                  'explanation':'Lower short interest is awarded more risk points; this is a screening rule, not a prediction.'},
        'sources':{'shortShares':'FINRA','floatShares':'Massive',
                   'finraTerms':'https://developer.finra.org/specific-terms-equity-data',
                   'floatDocumentation':'https://massive.com/docs/rest/stocks/fundamentals/float'},
        'coverage':{'identities':len(records),'finraRecords':len(short_by_key),'floatRecords':len(floats),
                    'statuses':status_counts},
        'data':records
    }



# Recently listed issuers are not necessarily in the main editorial SEC feed.
# Fetch only issuers added since the last SEC directory snapshot plus previously
# monitored new issuers, avoiding a 7,000-company SEC crawl.
SEC_SUBMISSIONS_BASE='https://data.sec.gov/submissions/CIK'
NEW_FILINGS_SCHEMA='mypybite.sec-new-issuer-filings.v1'
WATCH_FORMS={'S-1','S-1/A','424B4','8-A12B','8-K','10-Q','10-K','FWP'}

def recent_filings_for_issuer(submissions, ticker, cik, company):
    actual=cik10(submissions.get('cik'))
    if actual!=cik:
        raise ValueError('SEC submission CIK mismatch for '+ticker)
    filings=submissions.get('filings',{}).get('recent',{})
    acc=filings.get('accessionNumber',[])
    if not isinstance(acc,list):
        raise ValueError('SEC filings feed is malformed')
    rows=[]
    for i, accession in enumerate(acc[:100]):
        try:
            form=filings['form'][i]
            filed=filings['filingDate'][i]
            primary=filings['primaryDocument'][i]
        except (IndexError,KeyError,TypeError):
            continue
        if (form not in WATCH_FORMS or not strict_date(filed)
                or not re.fullmatch(r'\d{10}-\d{2}-\d{6}',str(accession))
                or not isinstance(primary,str)
                or not re.fullmatch(r'[A-Za-z0-9_./-]{1,140}',primary)
                or '..' in primary):
            continue
        url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/{primary}'
        rows.append({'ticker':ticker,'cik':cik,'companyName':company,
                     'form':form,'filedDate':filed,'accessionNumber':accession,
                     'acceptedAt':filings.get('acceptanceDateTime',[])[i] if i<len(filings.get('acceptanceDateTime',[])) else None,
                     'url':url,'signalLabel':'Verified SEC filing','readerNote':'Open the original SEC filing to examine the disclosure.',
                     'coverage':'new-issuer'})
    return rows[:35]


def refresh_new_issuer_filings(directory, old_dir_path, current_filings_path, getter=fetch_json):
    fields={field:i for i,field in enumerate(directory['fields'])}
    current={row[fields['symbol']]:(row[fields['cik']],row[fields['name']]) for row in directory['data']}
    old_symbols=set()
    if Path(old_dir_path).exists():
        previous=read_json(old_dir_path)
        ix={name:i for i,name in enumerate(previous.get('fields',[]))}
        if 'symbol' in ix:
            old_symbols={r[ix['symbol']] for r in previous.get('data',[]) if len(r)>ix['symbol']}
    already=set()
    if Path(current_filings_path).exists():
        prior=read_json(current_filings_path)
        if prior.get('schema')==NEW_FILINGS_SCHEMA:
            already={item.get('ticker') for item in prior.get('data',[]) if item.get('ticker') in current}
    # ANT is a known newly listed SEC issuer as of 2026-11-19.
    # The recurring job covers it until its first normal quarterly report.
    incoming=set(current).difference(old_symbols)
    wanted={symbol for symbol in incoming.union(already).union({'ANT'}) if symbol in current}
    # Limit SEC requests and prioritize newly filed, high-CIK issuers.
    ordered=sorted(wanted,key=lambda symbol:(symbol!='ANT',-int(current[symbol][0]),symbol))[:35]
    collected=[]
    for ticker in ordered:
        cik, company=current[ticker]
        payload=getter(f'{SEC_SUBMISSIONS_BASE}{cik}.json')
        collected.extend(recent_filings_for_issuer(payload,ticker,cik,company))
    return {'schema':NEW_FILINGS_SCHEMA,
            'generatedAt':datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z'),
            'source':'U.S. Securities and Exchange Commission, company submissions',
            'issuersIncluded':len(ordered),
            'data':sorted(collected,key=lambda row:(row['filedDate'],row['accessionNumber']),reverse=True)}

def write_atomic(destination, payload):
    path=Path(destination)
    path.parent.mkdir(parents=True,exist_ok=True)
    candidate=path.with_suffix(path.suffix+'.tmp')
    candidate.write_text(json.dumps(payload,separators=(',',':'),ensure_ascii=False)+'\n',encoding='utf-8')
    candidate.replace(path)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sec-json',help='Test-only local SEC fixture; default is official SEC URL')
    parser.add_argument('--float-json',help='Test-only local Massive fixture; default is API using MASSIVE_API_KEY')
    parser.add_argument('--finra-json',default='data/finra/display.json')
    parser.add_argument('--directory-out',default='outputs/zero-cost-ticker-directory-development.json')
    parser.add_argument('--assessment-out',default='data/finra/assessments.json')
    parser.add_argument('--new-filings-out',default='data/sec/filings/new-issuer-recent.json')
    parser.add_argument('--skip-sec-submissions',action='store_true',help='For deterministic offline tests only')
    args=parser.parse_args(argv)
    try:
        sec=read_json(args.sec_json) if args.sec_json else fetch_json(SEC_DIRECTORY_URL)
        directory=directory_from_sec(sec)
        finra=read_json(args.finra_json)
        floats=read_json(args.float_json) if args.float_json else retrieve_massive_float(os.environ.get('MASSIVE_API_KEY',''))
        assessment=build_assessment(directory,finra,floats)
        if assessment['coverage']['statuses'].get('scored',0)==0:
            raise ValueError('No fully scored short interest records; refusing to replace existing files')
        # Validate the SEC new-issuer feed before changing any existing files.
        new_filings = None if args.skip_sec_submissions else refresh_new_issuer_filings(
            directory,args.directory_out,args.new_filings_out)
        # Only write after all required sources have been completely validated.
        if new_filings is not None:
            write_atomic(args.new_filings_out,new_filings)
        write_atomic(args.directory_out,directory)
        write_atomic(args.assessment_out,assessment)
        print('SEC identities:',assessment['coverage']['identities'])
        print('FINRA securities:',assessment['coverage']['finraRecords'])
        print('Massive float securities:',assessment['coverage']['floatRecords'])
        print('Date-aligned scores:',assessment['coverage']['statuses'].get('scored',0))
        print('New SEC issuer filings:',len(new_filings['data']) if new_filings is not None else 'offline test skipped')
        print('Output:',args.directory_out, 'and', args.assessment_out)
    except Exception as exc:
        print('Signal evidence update aborted safely:',type(exc).__name__,str(exc),file=sys.stderr)
        return 1
    return 0

if __name__=='__main__':
    raise SystemExit(main())
