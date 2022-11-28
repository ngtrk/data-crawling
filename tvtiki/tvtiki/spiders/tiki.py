import scrapy


class TikiSpider(scrapy.Spider):
    name = 'tiki'
    allowed_domains = ['tiki.vn']
    start_urls = ['https://tiki.vn/tivi/c5015']
    custom_settings = {
        'FEEDS': { 'data.csv': { 'format': 'csv','overwrite': True } } }


    def get_discount(self, item):
        discount = '0'
        if item is not None:
            discount = item.split('<!-- -->')[1]
        return discount

    def freeship_installment(self, type, item):
        installment = 'no'
        freeship = 'no'
        if item is not None:
            for i in item:
                if i.css('div.item').css('span::text').get()[0] == 'F':
                    freeship = 'yes'
                elif i.css('div.item').css('span::text').get()[0] == 'T':
                    installment = 'yes'
        if type == 0:
            return freeship
        else:
            return installment


    def parse(self, response):
        for item in response.css('a.product-item'):
            if item.css('div.name').css('h3::text').get().find('.') == -1:
                yield {
                    'name': item.css('div.name').css('h3::text').get(),
                    'price' : item.css('div.price-discount__price::text').get(),
                    'discount' : self.get_discount(item.css('div.price-discount__discount').get()),
                    'rating' : item.css('div.total').css('span::text').get(),
                    'freeship' : self.freeship_installment(0, item.css('div.badge-under-rating').css('div.item')),
                    'tra_gop' :self.freeship_installment(1, item.css('div.badge-under-rating').css('div.item')),
                    'link' : 'tiki.vn' + item.css('a.product-item::attr(href)').get()
                }

        last_page = int(response.css('a[data-view-label]::attr(href)')[-2].get().split('=')[-1])
        for page in range(2, last_page + 1):
            next_page = f'https://tiki.vn/tivi/c5015?page={page}'
            yield response.follow(next_page, self.parse)   


