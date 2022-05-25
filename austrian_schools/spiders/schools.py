import re
from typing import Tuple, Optional, NamedTuple

import scrapy
import scrapy.signals


class SearchSummary(NamedTuple):
    from_: int
    to: int
    sum: int


class SchoolsSpider(scrapy.Spider):
    name = 'schools'
    allowed_domains = ['www.schulen-online.at']
    start_urls = ['https://www.schulen-online.at/sol/oeff_suche_schulen.jsf']

    def __init__(self, continue_from: str, **kwargs):
        super().__init__(**kwargs)
        self.lands = None
        self.results_per_page = 50
        if continue_from:
            self.continue_from = process_output_file(continue_from)

    def parse(self, response: scrapy.http.HtmlResponse, land_idx=0, continue_from_code: Optional[str] = None):
        self.logger.info('parse (search form): land: %i; continue_from: %s' % (land_idx, continue_from_code))
        if not self.lands:
            self.logger.debug('Load lands')
            self.lands = response.css('#myform1\\:bundesland option').xpath('text()').getall()[1:]

        if self.continue_from:
            land_idx = self.lands.index(self.continue_from[0])
            continue_from_code = self.continue_from[1]
        else:
            continue_from_code = None
        self.continue_from = None

        yield scrapy.FormRequest.from_response(
            response,
            'myform1',
            formdata={
                'myform1:anz': str(self.results_per_page),
                'myform1:bundesland': str(land_idx + 1),
                # there's a bug somewhere, scrapy sets here "on"
                'myform1:art': "",
            },
            callback=self.handle_search_results,
            cb_kwargs={
                'land_idx': land_idx,
                'continue_from_code': continue_from_code
            },
        )

    def handle_search_results(
            self, response: scrapy.http.HtmlResponse, idx=0, land_idx=0, continue_from_code: Optional[str] = None
    ):
        self.logger.info('land: %i; idx: %i; continue_from: %s' % (land_idx, idx, continue_from_code))
        tbl = response.xpath('//div[@id="tabs-2"]//table[@class="ergebnisTable"]/tbody')
        results_on_page = int(float(tbl.xpath('count(tr)').get()))

        if continue_from_code:
            continue_from_idx = tbl.xpath('count(tr[.//a/text()=$code]/preceding-sibling::tr)',
                                          code=continue_from_code).get()

            if continue_from_idx is not None:
                continue_from_idx = int(float(continue_from_idx))
                self.logger.info('Found item: %i' % continue_from_idx)
                # continue CRAWLING with the next row on this page
                yield self.continue_search_results(
                    response,
                    continue_from_idx,
                    land_idx,
                    last_item=continue_from_idx >= results_on_page - 1
                )
            else:
                # continue SEARCHING for the first page to crawl
                yield self.continue_search_results(
                    response,
                    results_on_page - 1,
                    land_idx,
                    continue_from_code,
                    last_item=True
                )

            return

        yield self.parse_search_results(response, idx, land_idx)

    def parse_search_results(self, response: scrapy.http.HtmlResponse, idx=0, land_idx=0):
        parent = response.xpath('//div[@id="tabs-2"]//table[@class="ergebnisTable"]/tbody')
        results_on_page = int(float(parent.xpath('count(tr)').get()))

        # 1-based
        onclick = parent.xpath('tr[$index]/td//a/@onclick', index=idx + 1).get()
        self.logger.debug(onclick)

        form_id, idcl = re.match(r"^return myfaces.oam.submitForm\('([^']+)','([^']+)'\);$", onclick).groups()
        return scrapy.FormRequest.from_response(
            response,
            form_id,
            formdata={
                'j_id_20:_idcl': idcl,
            },
            callback=self.parse_school,
            cb_kwargs={
                'idx': idx,
                'land_idx': land_idx,
                'last_item': idx == results_on_page - 1,
            },
        )

    def handle_school_details(self, response: scrapy.http.HtmlResponse, idx: int, land_idx: int,
                              last_item: bool = False):
        yield self.parse_school(response, idx, land_idx)

        next_ = self.continue_search_results(response, idx, land_idx, last_item=last_item)
        if next_:
            yield next_

    def parse_school(self, response: scrapy.http.HtmlResponse, idx: int, land_idx: int):
        search_summary = self.get_search_summary(response)
        self.logger.info('land: %i; school: %i / %i' % (land_idx, search_summary.from_ + idx, search_summary.sum))
        item = {
            'Bundesland': self.lands[land_idx]
        }
        rows = response.selector.css('#tabs-3 .rahmen_tab').xpath('div/div').xpath('h5|div')
        for row_idx in range(0, len(rows), 2):
            key = rows[row_idx].xpath('text()').get().strip()
            val_row = rows[row_idx + 1]
            if key == 'Homepage':
                value = val_row.xpath('a/@href').get()
            else:
                value = val_row.xpath('a/text()|text()').get()
                if value:
                    value = value.strip()
            item[key] = value
        yield item

    def continue_search_results(self, response, idx: int, land_idx: int, continue_from_code: Optional[str] = None,
                                last_item: bool = False) -> Optional[scrapy.Request]:
        """
        The only occasion when continue_from_code is passed here, is when searching for the first page after the last
        item in the input file, meaning, we only use it for the next page.

        :param last_item - the last processed item was last on the page
        """

        if last_item:
            if self.is_last_page(response):
                # next bundesland
                if land_idx + 1 < len(self.lands):
                    return self.parse(response, land_idx + 1).__next__()

            else:
                # next page
                return scrapy.FormRequest.from_response(
                    response,
                    'j_id_20',
                    formdata={
                        'j_id_20:_idcl': "j_id_20:next"
                    },
                    callback=self.parse_search_results,
                    cb_kwargs={
                        'land_idx': land_idx,
                        'continue_from_code': continue_from_code,
                    },
                )
        else:
            # next school
            return self.parse_search_results(response, idx + 1, land_idx)

    def is_last_page(self, response: scrapy.http.HtmlResponse) -> bool:
        summary = self.get_search_summary(response)
        return summary.to == summary.sum

    @staticmethod
    def get_search_summary(response: scrapy.http.HtmlResponse) -> SearchSummary:
        parent = response.css('#j_id_20 .buttonframe')
        return SearchSummary(
            int(parent.xpath('.//*[@id="j_id_20:from"]/text()').get()),
            int(parent.xpath('.//*[@id="j_id_20:to"]/text()').get()),
            int(parent.xpath('.//*[@id="j_id_20:sum"]/text()').get()),
        )


def process_output_file(path: str) -> Tuple[str, str]:
    with open(path, 'r') as fp:
        for count, line in enumerate(fp):
            pass
    return tuple(line.split(',')[:2])
