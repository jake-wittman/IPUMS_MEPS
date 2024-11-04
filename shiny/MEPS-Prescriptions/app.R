#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#page
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

library(shiny)
library(arrow)
library(dplyr)
library(ggplot2)
library(stringr)
library(DT)
library(bslib)

if (getwd() == "C:/R_Rstudio/Projects/IPUMS_MEPS/shiny/MEPS-Prescriptions") {
  dat <- arrow::read_parquet(here::here('shiny/MEPS-Prescriptions/data/person_drug_expenditures.parquet'))
  raw_dat <- tidytable::as_tidytable(
    arrow::read_parquet(here::here('shiny/MEPS-Prescriptions/data/raw_person_drug_expenditures.parquet'))
  )

} else {
  dat <- arrow::read_parquet(here::here('data/person_drug_expenditures.parquet'))
  raw_dat <- tidytable::as_tidytable(
    arrow::read_parquet(here::here('data/raw_person_drug_expenditures.parquet'))
  )

}
`%!in%` <- Negate(`%in%`)



dat <- dat |>
  rename(drug = Level,
         Median = oop_cost_30_day_supply) |>
  mutate(year = str_sub(year, 3, 4)) %>%
  mutate(
    insulin_indicator = case_when(
      drug %in% c(
        'Basal',
        'Bolus',
        'Human',
        'Analog',
        'Pre-mixed',
        'Human and Analog'
      ) ~ 'Insulin',
      drug %in% c(
        'Basal/Analog',
        'Basal/Human',
        'Bolus/Analog',
        'Bolus/Human',
        'Pre-Mixed/Human',
        'Pre-Mixed/Analog',
        'Pre-Mixed/Human And Analog'
      ) ~ 'Insulin',
      str_detect(drug, 'Insulin') ~ 'Insulin',
      TRUE ~ 'Not Insulin'
    )
  ) |>
  filter(
    drug != 'Other Oral Medications',
    drug %!in% c('Insulin/Other', 'Unknown', 'Unkown/Human', 'NA', '<N/A>'),
    !is.na(drug),
    age %in% c('Crude', '< 18', '18 - 44', '45 - 64', '65 - 74', '75+'),
    drug != 'None',
    !is.na(Median)
  )
# Define UI for application that draws a histogram
ui <- page_sidebar(

    # Application title
    title = "MEPS Prescription Data",

    # Sidebar with a slider input for number of bins
    sidebar = sidebar(

            selectInput('stratifier',
                        'Select stratifier',
                        c('Overall', 'Insurance'),
                        selected = 'Overall'),
            selectInput('drugs',
                        'Select drugs',
                        dat |> arrange(insulin_indicator, drug) |> pull(drug) |> unique(),
                        selected = c('Basal/Analog', 'Basal/Human', 'Bolus/Analog', 'Bolus/Human'),
                        multiple = TRUE),

            radioButtons('free_y',
                         'Use same y-axis across all plots?',
                         choices = c('Yes' = 'fixed',
                                     'No' = 'free_y')),
            numericInput('num_columns',
                         'Number of columns for plots',
                         min = 1,
                         max = 4,
                         value = 3),
            numericInput('years',
                         'Year to show for raw data',
                         min = 2003,
                         max = 2021,
                         step = 1,
                         value = 2021)
        ),

        # Show a plot of the generated distribution
        navset_card_underline(
          title = 'Select tabs on the right to view plots or tables.',

          nav_panel('Plot',
                    p('Average amount spent by individuals with diabetes on various prescription medications (standardized to
                      a 30 day prescription).
                      Choose which drugs to display by selecting or removing drugs from the sidebar
                      (select a drug and hit the "Backspace" or "Delete" button
                      to remove a drug). Plots can either show overall or stratified by
                      insurance coverage in the sidebar as well.'),
                    plotOutput("drug_plot")),

           nav_panel('Plot Data',
                     p('The data used to generate the plots. These are survey-adjusted estimates. You can use the search bar on the right
                       to search within drugs selected in the sidebar. If a drug is not selected in the side-bar, it will not show up here.'),
                     dataTableOutput('survey_adjusted_table')),

           nav_panel('Raw Data',
                     p('Raw data directly from the MEPS survey. Filter using the boxes at the top. Will only show drugs selected in the sidebar.'),
                     dataTableOutput('raw_table'))
        )

)

# Define server logic required to draw a histogram
server <- function(input, output) {
  plot_dat <- reactive({
    dat |>
      filter(drug %in% input$drugs,
             stratifier %in% input$stratifier)
  })
  raw_table_dat <- reactive({
    raw_dat |>
      tidytable::filter(drug %in% input$drugs) |>
      as_tibble()
  })
  plot_height <- reactive(input$plot_height)
  plot_width <- reactive(input$plot_width)
  output$drug_plot <- renderPlot({
    ggplot(data = plot_dat(),
           aes(x = year, y = Median, color = group, group = group)) +
       geom_line() +
      facet_wrap( ~ drug,
                  scales = input$free_y,
                  ncol = input$num_columns,
                  axes = 'all_x') +
      theme_minimal() +
      theme(axis.text.x = element_text(vjust = 0.5),
            axis.text = element_text(size = 14),
            axis.title = element_text(size = 16),
            strip.text = element_text(size = 16),
            legend.text = element_text(size = 12),
            plot.caption = element_text(hjust = 0, size = 12)) +
      labs(
        y = 'CPI Adjusted $',
        caption = str_wrap(
          'Median amount spent out-of-pocket per person for a 30 day supply of each type of medication.

           Dollar amounts have been adjusted to 2021 dollars. ',
          80
        )
      )

    },
    height = 800)

  output$survey_adjusted_table <- renderDataTable({
    plot_dat()
  },
  options = list(pageLength = 10),
  filter = 'top')

  output$raw_table <- renderDataTable({
    raw_table_dat()
  },
  options = list(pageLength = 10),
  filter = 'top')
}

# Run the application
shinyApp(ui = ui, server = server)
