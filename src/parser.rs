use std::error::Error as StdError;
use std::{str::FromStr, sync::LazyLock};

use chrono::NaiveDate;
use color_eyre::eyre::anyhow;
use color_eyre::{eyre::Context, Result};
use regex::{Captures, Regex};
use rust_decimal::Decimal;

use crate::Posting;

/// Iterator which parses lines from a Sparkassen Danmark PDF export
///
/// The format of the document is as follows:
///
/// 1. A prelude, constituted of any number of lines to ignore
/// 2. A number of pages
///
/// Each page starts with the line `"Side X af Y"` with X being the index of the current page
/// (1-based) and Y the total number of pages.
///
/// This is followed by a header `"Dato Posteringstekst Beløb Saldo Valuta"`
/// The header is followed by a series of postings like so:
/// `"23/08/2023 MobilePay: MobilePay Johnny Olsen -250,00 DKK 73.900,69 DKK DKK"`
///
/// The posting may be over multiple lines, depending on the description.
///
/// There may also be a blank line between two postings, or around page separators or headers
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct PostingParser<I> {
    iter: I,
    header_seen: bool,
    fatal_error_seen: bool,
    previous_line: Option<String>,
}

const PDF_TABLE_HEADER: &str = "Dato Posteringstekst Beløb Saldo Valuta";
const DAY_FIELD: &str = "day";
const MONTH_FIELD: &str = "month";
const YEAR_FIELD: &str = "year";
const DESCRIPTION_FIELD: &str = "description";
const AMOUNT_SIGN_FIELD: &str = "amount_sign";
const AMOUNT_INT_PART_FIELD: &str = "amount_int_part";
const AMOUNT_FRACT_PART_FIELD: &str = "amount_fract_part";
const BALANCE_SIGN_FIELD: &str = "balance_sign";
const BALANCE_INT_PART_FIELD: &str = "balance_int_part";
const BALANCE_FRACT_PART_FIELD: &str = "balance_fract_part";
const CURRENCY_FIELD: &str = "currency";

impl<I> PostingParser<I> {
    fn new(iter: I) -> Self {
        Self {
            iter,
            header_seen: false,
            fatal_error_seen: false,
            previous_line: None,
        }
    }

    fn try_parse_posting(s: &str) -> Result<PostingParseResult> {
        static POSTING_REGEX: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(
                &format!(
                r"(?x)
                (?P<{DAY_FIELD}>\d{{2}})
                /
                (?P<{MONTH_FIELD}>\d{{2}})
                /
                (?P<{YEAR_FIELD}>\d{{4}})
                \x20
                (?P<{DESCRIPTION_FIELD}>.+?)
                \x20
                (?P<{AMOUNT_SIGN_FIELD}>-?)(?P<{AMOUNT_INT_PART_FIELD}>[\d\.]+),(?P<{AMOUNT_FRACT_PART_FIELD}>\d{{2}})\ DKK
                \x20
                (?P<{BALANCE_SIGN_FIELD}>-?)(?P<{BALANCE_INT_PART_FIELD}>[\d\.]+),(?P<{BALANCE_FRACT_PART_FIELD}>\d{{2}})\ DKK
                \x20
                (?P<{CURRENCY_FIELD}>[A-Z]+)
            ",
            )
            )
            .expect("regex")
        });

        match POSTING_REGEX.captures(s) {
            Some(groups) => {
                let date = Self::parse_date(&groups)?;

                let description = groups[DESCRIPTION_FIELD].to_string();

                let amount = Self::parse_amount(&groups)?;

                let balance = Self::parse_balance(&groups)?;

                let currency = groups[CURRENCY_FIELD].to_owned();

                Ok(PostingParseResult::PatternMatched(Posting {
                    date,
                    description,
                    amount,
                    balance,
                    currency,
                }))
            }
            None => Ok(PostingParseResult::PatternNotMatched),
        }
    }

    fn parse_date(groups: &Captures<'_>) -> Result<NaiveDate> {
        let day: u32 = Self::parse_integer_field(DAY_FIELD, groups)?;
        let month: u32 = Self::parse_integer_field(MONTH_FIELD, groups)?;
        let year: i32 = Self::parse_integer_field(YEAR_FIELD, groups)?;
        NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
            anyhow!("Could not build a valid date from year={year}, month={month}, day={day}")
        })
    }

    fn parse_integer_field<T: FromStr<Err = E>, E: StdError + Send + Sync + 'static>(
        key: &str,
        groups: &Captures<'_>,
    ) -> Result<T> {
        groups[key]
            .parse::<T>()
            .with_context(|| format!("Could not parse {key} '{}'", &groups[key]))
    }

    fn parse_amount(groups: &Captures<'_>) -> Result<Decimal> {
        Self::parse_floating_field_group(
            AMOUNT_SIGN_FIELD,
            AMOUNT_INT_PART_FIELD,
            AMOUNT_FRACT_PART_FIELD,
            "amount",
            groups,
        )
    }

    fn parse_balance(groups: &Captures<'_>) -> Result<Decimal> {
        Self::parse_floating_field_group(
            BALANCE_SIGN_FIELD,
            BALANCE_INT_PART_FIELD,
            BALANCE_FRACT_PART_FIELD,
            "balance",
            groups,
        )
    }

    fn parse_floating_field_group(
        sign_field: &str,
        int_part_field: &str,
        fractional_part_field: &str,
        field_name: &str,
        groups: &Captures<'_>,
    ) -> Result<Decimal> {
        let field_string_value = format!(
            "{}{}.{}",
            &groups[sign_field],
            &groups[int_part_field].replace('.', ""),
            &groups[fractional_part_field]
        );
        field_string_value
            .parse()
            .with_context(|| format!("Could not parse {field_name} '{field_string_value}'"))
    }
}

impl<'a, I: Iterator<Item = &'a str>> Iterator for PostingParser<I> {
    type Item = Result<Posting>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.fatal_error_seen {
            return None;
        }
        for line in self.iter.by_ref() {
            let current_line_is_header = line == PDF_TABLE_HEADER;
            if current_line_is_header {
                if self.previous_line.is_some() {
                    self.fatal_error_seen = true;
                    return Some(Err(anyhow!(
                        "Header line '{line}' after a partial posting line"
                    )));
                }
                self.header_seen = true;
                continue;
            }
            if self.header_seen {
                if line.is_empty() || line.starts_with("Side ") {
                    if self.previous_line.is_some() {
                        self.fatal_error_seen = true;
                        return Some(Err(anyhow!(
                            "Non-posting line '{line}' after a partial posting line {:?}",
                            self.previous_line
                        )));
                    }
                    continue;
                }
                match Self::try_parse_posting(line) {
                    Ok(result) => {
                        match result {
                            PostingParseResult::PatternMatched(posting) => {
                                return Some(Ok(posting));
                            }
                            PostingParseResult::PatternNotMatched => {
                                if let Some(previous_lines) = self.previous_line.as_mut() {
                                    previous_lines.push_str(line);
                                    match Self::try_parse_posting(previous_lines) {
                                        Ok(result) => match result {
                                            PostingParseResult::PatternMatched(posting) => {
                                                self.previous_line = None;
                                                return Some(Ok(posting));
                                            }
                                            PostingParseResult::PatternNotMatched => {
                                                continue;
                                            }
                                        },
                                        Err(err) => {
                                            return Some(Err(err).with_context(|| {
                                            format!("Could not parse {previous_lines} (combination)")
                                        }));
                                        }
                                    }
                                } else {
                                    self.previous_line = Some(line.to_string());
                                }
                            }
                        }
                    }
                    Err(err) => {
                        return Some(Err(err).with_context(|| format!("Could not parse {line}")));
                    }
                }
            } else {
                continue;
            }
        }
        if self.header_seen || self.fatal_error_seen {
            None
        } else {
            self.fatal_error_seen = true;
            Some(Err(anyhow!("No header seen")))
        }
    }
}

pub enum PostingParseResult {
    PatternMatched(Posting),
    PatternNotMatched,
}

pub trait IteratorExt {
    fn postings(self) -> PostingParser<Self>
    where
        Self: Sized;
}

impl<'a, I: Iterator<Item = &'a str>> IteratorExt for I {
    fn postings(self) -> PostingParser<I> {
        PostingParser::new(self)
    }
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;

    use super::Posting;

    use helpers::*;
    use rust_decimal_macros::dec;

    #[test]
    fn should_parse_multiple_pages() {
        let postings = parse_postings(&pages_with_prelude(PRELUDE, &[
                &[
                    "16/11/2023 kontaktløs Dankort Bagerhuset Nota 010101 -50,00 DKK 84.411,22 DKK DKK",
                    "15/11/2023 MobilePay: MobilePay Johnny Olsen -485,00 DKK 84.461,22 DKK DKK",
                    "15/11/2023 MobilePay: MobilePay Johnny Olsen -48,00 DKK 84.946,22 DKK DKK",
                    "15/11/2023 MobilePay: MobilePay Johnny Olsen 48,00 DKK 85.004,22 DKK DKK",
                    "15/11/2023 MobilePay: MobilePay Johnny Olsen -817,00 DKK 84.946,22 DKK DKK",
                    "13/11/2023 Visa/Dankort PAYPAL *SPOTIFY, 01010101010, Nota nr.",
                    "01010 -179,00 DKK 85.763,22 DKK DKK",
                    "",
                    "12/11/2023 Visa/Dankort PAYPAL *ADMIN, 01010101010, Nota nr.",
                    "01010 -18,17 DKK 85.942,22 DKK DKK",
                    "",
                    "11/11/2023 kontaktløs Dankort Some Shop Nota 010101 -153,00 DKK 85.960,39 DKK DKK",
                ],
                &[
                    "11/11/2023 kontaktløs Dankort Some Shop Nota 010101 -111,00 DKK 86.113,39 DKK DKK",
                    "03/11/2023 Cash infusion 92.113,39 DKK 86.224,39 DKK DKK",
                    "02/11/2023 Cash infusion 4.000,00 DKK -6.000,00 DKK DKK",
                    "01/11/2023 Cash withdrawal -96.113,39 DKK -10.000,00 DKK DKK",
                ],
        ]).collect::<Vec<_>>());
        let postings = postings.expect("postings");
        assert_eq!(
            vec![
                Posting {
                    date: date(2023, 11, 16),
                    description: "kontaktløs Dankort Bagerhuset Nota 010101".to_owned(),
                    amount: dec!(-50.00),
                    balance: dec!(84_411.22),
                    currency: "DKK".to_owned()
                },
                Posting {
                    date: date(2023, 11, 15),
                    description: "MobilePay: MobilePay Johnny Olsen".to_owned(),
                    amount: dec!(-485.00),
                    balance: dec!(84_461.22),
                    currency: "DKK".to_owned()
                },
                Posting {
                    date: date(2023, 11, 15),
                    description: "MobilePay: MobilePay Johnny Olsen".to_owned(),
                    amount: dec!(-48.00),
                    balance: dec!(84_946.22),
                    currency: "DKK".to_owned()
                },
                Posting {
                    date: date(2023, 11, 15),
                    description: "MobilePay: MobilePay Johnny Olsen".to_owned(),
                    amount: dec!(48.00),
                    balance: dec!(85_004.22),
                    currency: "DKK".to_owned()
                },
                Posting {
                    date: date(2023, 11, 15),
                    description: "MobilePay: MobilePay Johnny Olsen".to_owned(),
                    amount: dec!(-817.00),
                    balance: dec!(84_946.22),
                    currency: "DKK".to_owned()
                },
                Posting {
                    date: date(2023, 11, 13),
                    description: "Visa/Dankort PAYPAL *SPOTIFY, 01010101010, Nota nr.01010"
                        .to_owned(),
                    amount: dec!(-179.00),
                    balance: dec!(85_763.22),
                    currency: "DKK".to_owned()
                },
                Posting {
                    date: date(2023, 11, 12),
                    description: "Visa/Dankort PAYPAL *ADMIN, 01010101010, Nota nr.01010"
                        .to_owned(),
                    amount: dec!(-18.17),
                    balance: dec!(85_942.22),
                    currency: "DKK".to_owned()
                },
                Posting {
                    date: date(2023, 11, 11),
                    description: "kontaktløs Dankort Some Shop Nota 010101".to_owned(),
                    amount: dec!(-153.00),
                    balance: dec!(85_960.39),
                    currency: "DKK".to_owned()
                },
                Posting {
                    date: date(2023, 11, 11),
                    description: "kontaktløs Dankort Some Shop Nota 010101".to_owned(),
                    amount: dec!(-111.00),
                    balance: dec!(86_113.39),
                    currency: "DKK".to_owned()
                },
                Posting {
                    date: date(2023, 11, 3),
                    description: "Cash infusion".to_owned(),
                    amount: dec!(92_113.39),
                    balance: dec!(86_224.39),
                    currency: "DKK".to_owned()
                },
                Posting {
                    date: date(2023, 11, 2),
                    description: "Cash infusion".to_owned(),
                    amount: dec!(4_000.00),
                    balance: dec!(-6_000.00),
                    currency: "DKK".to_owned()
                },
                Posting {
                    date: date(2023, 11, 1),
                    description: "Cash withdrawal".to_owned(),
                    amount: dec!(-96_113.39),
                    balance: dec!(-10_000.00),
                    currency: "DKK".to_owned()
                },
            ],
            postings
        );
    }

    #[test]
    fn should_parse_page_with_a_single_transaction() {
        let postings = parse_postings(
            &pages_with_prelude(
                PRELUDE,
                &[&["03/11/2023 Cash infusion 92.113,39 DKK 86.224,39 DKK DKK"]],
            )
            .collect::<Vec<_>>(),
        );
        let postings = postings.expect("postings");
        assert_eq!(
            vec![Posting {
                date: date(2023, 11, 3),
                description: "Cash infusion".to_owned(),
                amount: dec!(92_113.39),
                balance: dec!(86_224.39),
                currency: "DKK".to_owned()
            },],
            postings
        );
    }

    #[test]
    fn should_parse_page_with_a_single_multiline_transaction() {
        let postings = parse_postings(
            &pages_with_prelude(
                PRELUDE,
                &[&[
                    "13/11/2023 Visa/Dankort PAYPAL *SPOTIFY, 01010101010, Nota nr.",
                    "01010 -179,00 DKK 85.763,22 DKK DKK",
                    "",
                ]],
            )
            .collect::<Vec<_>>(),
        );
        let postings = postings.expect("postings");
        assert_eq!(
            vec![Posting {
                date: date(2023, 11, 13),
                description: "Visa/Dankort PAYPAL *SPOTIFY, 01010101010, Nota nr.01010".to_owned(),
                amount: dec!(-179.00),
                balance: dec!(85_763.22),
                currency: "DKK".to_owned()
            },],
            postings
        );
    }

    #[test]
    fn should_not_parse_statements_with_an_invalid_date() {
        let mut postings = parse_postings_as_vec_of_results(
            &pages_with_prelude(
                PRELUDE,
                &[&[
                    "12/31/2023 Cash infusion 92.113,39 DKK 86.224,39 DKK DKK",
                    "13/11/2023 Visa/Dankort PAYPAL *SPOTIFY, 01010101010, Nota nr.",
                    "01010 -179,00 DKK 85.763,22 DKK DKK",
                    "",
                ]],
            )
            .collect::<Vec<_>>(),
        );
        assert_eq!(2, postings.len());
        assert!(postings[0].is_err());
        let posting = postings.pop().expect("posting").expect("posting");
        assert_eq!(
            Posting {
                date: date(2023, 11, 13),
                description: "Visa/Dankort PAYPAL *SPOTIFY, 01010101010, Nota nr.01010".to_owned(),
                amount: dec!(-179.00),
                balance: dec!(85_763.22),
                currency: "DKK".to_owned()
            },
            posting
        );
    }

    #[test]
    fn should_not_parse_statements_with_an_invalid_amount() {
        let mut postings = parse_postings_as_vec_of_results(
            &pages_with_prelude(
                PRELUDE,
                &[&[
                    "12/11/2023 Cash infusion 92.17777777777777777777777777777777713,39 DKK 86.224,39 DKK DKK",
                    "13/11/2023 Visa/Dankort PAYPAL *SPOTIFY, 01010101010, Nota nr.",
                    "01010 -179,00 DKK 85.763,22 DKK DKK",
                    "",
                ]],
            )
            .collect::<Vec<_>>(),
        );
        assert_eq!(2, postings.len());
        assert!(postings[0].is_err());
        let posting = postings.pop().expect("posting").expect("posting");
        assert_eq!(
            Posting {
                date: date(2023, 11, 13),
                description: "Visa/Dankort PAYPAL *SPOTIFY, 01010101010, Nota nr.01010".to_owned(),
                amount: dec!(-179.00),
                balance: dec!(85_763.22),
                currency: "DKK".to_owned()
            },
            posting
        );
    }

    #[test]
    fn should_not_parse_statements_with_an_invalid_balance() {
        let mut postings = parse_postings_as_vec_of_results(
            &pages_with_prelude(
                PRELUDE,
                &[&[
                    "12/11/2023 Cash infusion 92.113,39 DKK 86.277777777777777777777777777777777724,39 DKK DKK",
                    "13/11/2023 Visa/Dankort PAYPAL *SPOTIFY, 01010101010, Nota nr.",
                    "01010 -179,00 DKK 85.763,22 DKK DKK",
                    "",
                ]],
            )
            .collect::<Vec<_>>(),
        );
        assert_eq!(2, postings.len());
        assert!(postings[0].is_err());
        let posting = postings.pop().expect("posting").expect("posting");
        assert_eq!(
            Posting {
                date: date(2023, 11, 13),
                description: "Visa/Dankort PAYPAL *SPOTIFY, 01010101010, Nota nr.01010".to_owned(),
                amount: dec!(-179.00),
                balance: dec!(85_763.22),
                currency: "DKK".to_owned()
            },
            posting
        );
    }

    #[test]
    fn should_not_parse_statements_with_a_multiline_transaction_split_over_two_pages() {
        let mut postings = parse_postings_as_vec_of_results(
            &pages_with_prelude(
                PRELUDE,
                &[
                    &[
                        "12/11/2023 Cash infusion 92.113,39 DKK 86.224,39 DKK DKK",
                        "13/11/2023 Visa/Dankort PAYPAL *SPOTIFY, 01010101010, Nota nr.",
                    ],
                    &[
                        "01010 -179,00 DKK 85.763,22 DKK DKK",
                        "",
                        "12/31/2023 Cash infusion 92.113,39 DKK 86.224,39 DKK DKK",
                    ],
                ],
            )
            .collect::<Vec<_>>(),
        );
        assert_eq!(2, postings.len());

        assert!(postings[1].is_err());
        postings.pop();

        let posting = postings.pop().expect("posting").expect("posting");
        assert_eq!(
            Posting {
                date: date(2023, 11, 12),
                description: "Cash infusion".to_owned(),
                amount: dec!(92_113.39),
                balance: dec!(86_224.39),
                currency: "DKK".to_owned()
            },
            posting
        );
    }

    #[test]
    fn should_return_an_error_if_the_document_is_empty() {
        assert!(parse_postings::<&str>(&[]).is_err());
    }

    #[test]
    fn should_return_an_error_if_no_header_was_found() {
        assert!(parse_postings(&["foo", "bar"]).is_err());
    }

    #[test]
    fn should_parse_a_single_empty_page_correctly() {
        let postings = parse_postings(&pages_with_prelude(PRELUDE, &[&[""]]).collect::<Vec<_>>());
        let postings = postings.expect("postings");
        assert!(postings.is_empty());
    }

    #[test]
    fn should_parse_multiple_empty_pages_correctly() {
        // This should not be possible, but it should not error
        let postings = parse_postings(&pages_with_prelude(PRELUDE, &[&[""]]).collect::<Vec<_>>());
        let postings = postings.expect("postings");
        assert!(postings.is_empty());
    }

    mod helpers {
        use std::borrow::Cow;

        use color_eyre::Result;

        use super::super::*;

        pub(super) const PRELUDE: &[&str] = &[
            "02/03/2025",
            "",
            "Johnny Olsen",
            "",
            "Konto: 9070 1010101010 - SparD Plus (DKK)",
            "",
            "Udskriftsperiode (Kontoaktivitet): 01/08/2023 - 31/12/2023",
            "",
            "Udskriftsdato: 02/03/2025",
            "",
            "Kontobevægelser",
        ];

        pub(super) fn parse_postings<T: AsRef<str>>(lines: &[T]) -> Result<Vec<Posting>> {
            lines
                .iter()
                .map(|v| v.as_ref())
                .postings()
                .collect::<Result<Vec<Posting>>>()
        }

        pub(super) fn parse_postings_as_vec_of_results<T: AsRef<str>>(
            lines: &[T],
        ) -> Vec<Result<Posting>> {
            lines
                .iter()
                .map(|v| v.as_ref())
                .postings()
                .collect::<Vec<Result<Posting>>>()
        }

        pub(super) fn pages_with_prelude<'a>(
            prelude: &'a [&'a str],
            pages_content: &'a [&'a [&'a str]],
        ) -> Box<dyn Iterator<Item = Cow<'a, str>> + 'a> {
            Box::new(
                prelude
                    .iter()
                    .copied()
                    .map(Cow::from)
                    .chain(pages(pages_content)),
            )
        }

        fn pages<'a>(
            pages_content: &'a [&'a [&'a str]],
        ) -> Box<dyn Iterator<Item = Cow<'a, str>> + 'a> {
            Box::new(
                pages_content
                    .iter()
                    .enumerate()
                    .flat_map(|(i, page_content)| page(page_content, i + 1, pages_content.len())),
            )
        }

        fn page<'a>(
            posting_lines: &'a [&'a str],
            page_number: usize,
            total_page_count: usize,
        ) -> Box<dyn Iterator<Item = Cow<'a, str>> + 'a> {
            Box::new(
                ((["", PDF_TABLE_HEADER].iter())
                    .chain(posting_lines.iter())
                    .chain([""].iter()))
                .copied()
                .map(Cow::from)
                .chain([format!("Side {page_number} af {total_page_count}").into()]),
            )
        }

        pub(super) fn date(year: i32, month: u32, day: u32) -> NaiveDate {
            NaiveDate::from_ymd_opt(year, month, day).expect("valid date")
        }
    }
}
