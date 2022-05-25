import re

import scrapy


class SchoolsSpider(scrapy.Spider):
    name = 'schools'
    allowed_domains = ['www.schulen-online.at']
    start_urls = ['https://www.schulen-online.at/sol/oeff_suche_schulen.jsf']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lands = None

    def parse(self, response: scrapy.http.HtmlResponse, land_idx=0, **kwargs):
        if not self.lands:
            self.lands = response.css('#myform1\\:bundesland option').xpath('text()').getall()[1:]
            print(self.lands)

        request = scrapy.FormRequest.from_response(
            response,
            'myform1',
            formdata={
                'myform1:anz': '50',
                'myform1:bundesland': str(land_idx + 1),
                # there's a bug somewhere, scrapy sets here "on"
                'myform1:art': "",
            },
            callback=self.parse_search_results,
        )
        yield request

    def parse_search_results(self, response: scrapy.http.HtmlResponse, idx=0, land_idx=0):
        # 1-based
        onclick = response.xpath('//div[@id="tabs-2"]//table[@class="ergebnisTable"]/tbody/tr[$index]/td//a/@onclick',
                                 index=idx + 1).get()

        form_id, idcl = re.match(r"return myfaces.oam.submitForm\('([^']+)','([^']+)'\);", onclick).groups()
        yield scrapy.FormRequest.from_response(
            response,
            form_id,
            formdata={
                'j_id_20:_idcl': idcl,
            },
            callback=self.parse_school,
            cb_kwargs={
                'idx': idx,
                'land_idx': land_idx,
            },
        )

    def parse_school(self, response: scrapy.http.HtmlResponse, idx: int, land_idx):
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

        if idx < 49:
            for request in self.parse_search_results(response, idx + 1):
                yield request
        else:
            if self.is_last_page(response):
                for request in self.parse(response, land_idx + 1):
                    yield request
            else:
                yield scrapy.FormRequest.from_response(
                    response,
                    'j_id_20',
                    formdata={
                        'j_id_20:_idcl': "j_id_20:next"
                    },
                    callback=self.parse_search_results,
                )

    def is_last_page(self, response:scrapy.http.HtmlResponse)->bool:
        parent = response.css('#j_id_20 .buttonframe')
        last_page_idx=int(parent.xpath('.//*[@id="j_id_20:to"]/text()').get())
        last_idx=int(parent.xpath('.//*[@id="j_id_20:sum"]/text()').get())
        self.log('Page %i of %i'%(last_page_idx, last_idx))
        return last_page_idx == last_idx