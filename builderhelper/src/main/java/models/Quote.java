package models;

import util.Builder;

import java.math.BigDecimal;

@Builder
public class Quote {

    private BigDecimal bidQuote;
    private BigDecimal askQuote;

}
