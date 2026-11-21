# Onboarding → retention insight

**Insight.** Users who follow 5+ accounts in their first 24 hours retain at **40.2%** on day 7
vs **19.5%** for those who don't: **2.1×** (95% CI 2.01–2.12,
χ² = 2,837, p < 0.001, n = 56,544). It is the only step whose association
survives controlling for the other steps, channel, device and country (adjusted odds ratio 2.69,
95% CI 2.59–2.80); the other steps' raw lifts (1.13–1.26×)
shrink to adjusted odds ratios of 1.07–1.23.
Retention also falls steadily the later the follows happen (41% within the first hour vs
18% for users who never do it).

**Recommendation.** Onboarding should push follows earlier: put suggested accounts right after signup
(before the profile photo), pre-select popular and friends-of-friends accounts, and keep nudging users
who skip it in their first session.

**Limitations.** This is correlation, not causation. Users who are already more engaged both follow more
and retain more, and the regression only controls for what we observe. Day-7 activity is a single-day
metric, and the data is synthetic. **To confirm**, run an A/B test that prompts a random half of new users
to follow 5 accounts during onboarding and compare day-7 retention (sized in `ab_test_onboarding.ipynb`).
