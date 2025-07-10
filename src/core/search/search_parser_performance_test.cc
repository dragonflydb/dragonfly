// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/time/time.h>

#include <iomanip>
#include <iostream>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/search/base.h"
#include "core/search/query_driver.h"
#include "core/search/search.h"

namespace dfly::search {

// Performance test to reproduce the first-query initialization penalty issue.
// The issue can be reproduced only by running standalone, because the slow down is only visible
// when running the test standalone. The slowest query is the first one per process, and the
// subsequent ones are much faster.
TEST(SearchParserPerformanceTest, FirstQueryInitializationPenalty) {
  QueryParams params;  // No extra params needed for this query

  // Real production query that causes the issue (250 RETURN fields)
  std::string production_query =
      "* RETURN 250 lv_gwlurzpyxpsnv lv_urruguhbk lv_jagzzrtdeoo lv_unzcq lv_sstmwej "
      "lv_wrubdfagslzyb lv_srngqcllywgnexw lv_kdxtlmalghvk lv_affwkhxoyjodclj lv_qswukzvmihod "
      "lv_sleoufafhuo lv_obxzmh lv_milgkbtjiptpnw lv_lggauxre lv_nepeduiwshcwjvh lv_sfqnkypaemyq "
      "lv_csnengqetkcqj lv_sxkibautyjgckzi lv_gbpildmrcblepp lv_ehuqbrakh lv_xoepnylvvzcq "
      "lv_ojepjuhgbg lv_pjrthbpdjvpt lv_nlyaj lv_yfmxc lv_urxysxzppnwwbx lv_uxrctawab "
      "lv_wgpmkcpgoqovbd lv_tvzndr lv_iijoehxkmdliatz lv_jucyhgv lv_lxbddei lv_sznhcffea "
      "lv_mvrehzfqzb lv_peqqv lv_zxwhwwcj lv_mcpuivtut lv_wlktgv lv_lsgixcdjd lv_xikfuw "
      "lv_mfcikeantd lv_covrnsgj lv_teaswofztmynptl lv_gfwbnhvtipri lv_kvksiigy lv_mtycquib "
      "lv_zetasyfuvbwurd lv_zsepqqo lv_fivzhnsbedsscpe lv_unoewfb lv_yzxudwuhut lv_ylyeff "
      "lv_wshaycsmt lv_jbmfxj lv_qmqhymurhbblmf lv_ckjeqcnzrqs lv_nrgzakltgmu lv_ngzqmnbwe "
      "lv_rdzkuiytfnnbdi lv_ijftnzvzarlxpqy lv_xpjcjflkg lv_koyvfvxmma lv_ciqsetcsqcss "
      "lv_tzaknsyxwgbaoo lv_ibumfuqhulofkq lv_napztymi lv_xqqlivqu lv_epdwsoguh lv_cdmfjqgszlwdiu "
      "lv_wiziktncgd lv_vjrzpusramzu lv_vdtwmjhoe lv_ngdlpgkrczbnnzo lv_otoqzsrj lv_nlmeflph "
      "lv_rszpipzq lv_wzijrunp lv_zlagnnpolk lv_tifibwtaopsh lv_zacdylbfcaj lv_lhwtekhf "
      "lv_jfmmxdyl lv_capsbt lv_eruxprigdjdkpo lv_knwmwmcxsnxur lv_qfqhirzqynsid lv_rqbqstlccf "
      "lv_oeabpamipuie lv_lohzunr lv_efbaqrmikfag lv_jiftvcxtocusci lv_gfwqxksruwla "
      "lv_ttnjsmdwaylr lv_vthfoz lv_skijjormejy lv_nmbywdf lv_dgvtericjthu lv_pnlaidw "
      "lv_fxhxfetttxss lv_ejjpzgbq lv_aepihqtrnmmhmno lv_kvtpb lv_hdgqhdnpmsuup lv_diijapr "
      "lv_iebtntmuncnybu lv_zfwmyjjmkple lv_cwgxtehfirwngxb lv_ujhbodnfbtklpnf lv_conteaup "
      "lv_oofcim lv_kxddaj lv_dimtogklstm lv_nlewdggttfkcym lv_vniprumr lv_xdwcjhndhj lv_tzqkzjw "
      "lv_rkemdqakoaqdmbj lv_msljfleew lv_ryuqry lv_fjrudnzihbzqj lv_rvbymjxgyrx lv_sfxawsj "
      "lv_dezlgpmj lv_vmjpg lv_czwdwdzkdzv lv_eosmkcwqoxvldzj lv_nqhinfvnkqvrbss lv_sdslacgvosnc "
      "lv_igdervsbvicg lv_vktwa lv_tuote lv_wehjhi lv_wnrqlsmrqdvxpay lv_yudetrzrbaxlntn "
      "lv_mdgvwocu lv_tvqtdedazqpp lv_ekjcyltccloncco lv_mefkzyyfrffe lv_pfddtpkomn "
      "lv_rhzbuemawynfun lv_jdxmlhseejt lv_ybkmizqh lv_slahkgrjoizojlh lv_amaioicmr lv_oacpq "
      "lv_futmakdxqni lv_rzgkk lv_azsvpxcgrepoq lv_mmvwdcweenhxp lv_iqvrzmedsyafu lv_vryru "
      "lv_pnqkpb lv_uptfluaagcix lv_farfsvv lv_hbmfyiarxvr lv_zoucwdj lv_lyxqrdxnri lv_cafkumjs "
      "lv_uimtgrvufke lv_tckxkostdejobv lv_hzvsrdgbidgeo lv_zqlriwu lv_dcvrlypazr lv_omaagyvmqxkm "
      "lv_gqxit lv_goselkkfnpmwssn lv_zvzywemfkbjzgul lv_awqgjz lv_mjylnorwrugmyhh lv_bkjtjbgrcptd "
      "lv_zzuwz lv_bfytthdmldnfor lv_gcncreekcw lv_esupdarri lv_qngebdcll lv_uuecllms "
      "lv_glqftiqtdek lv_ftvezwl StateCode lv_xjsar lv_ynzogtejuw lv_usfpszfdb lv_sewuhmdw "
      "lv_cuuxlam lv_droizfbebh lv_ztzupwconedf lv_ikwscquidtykiz lv_mpypnrhadbkunxw lv_cgedtbyq "
      "lv_gsqvfcqnups lv_pexpwnt lv_uyqezsyufpe lv_jkezwcweuhtrdm lv_robiqjnskhengfw lv_zvoyzrhb "
      "lv_tygeuekeyukrtg lv_xarrkp lv_dfyakrymktae lv_gcaktzwmjo lv_qedffbjhoishgw lv_solybcw "
      "lv_jdnouecwqg lv_rlrwdpyadqmxbsd lv_bfmgttvkwxqvghe lv_fevaboeqbobg lv_wcnademrw "
      "lv_gwbcefgvc lv_kqhkzeylrjlclk lv_mwskkyqjmzefrcm lv_mnxyeeh lv_plrqgzweet lv_gyxbgpgnny "
      "lv_dscqqkxibrlh lv_rnaiyitezehunot lv_lxeokygwhqh lv_zipzdvshsp lv_ybtgggrnukxw "
      "lv_smhyzodoxlrtxdo lv_bhulepvcmufulvz lv_fbvxhkdyhmcxdl lv_mdbugqhgt lv_aistdz lv_avzdvj "
      "lv_akusozgnreg lv_whtmrjflttrhpo lv_cqklupl lv_epvrkic lv_zskzgz lv_chqxjebruzw lv_yiyptsk "
      "lv_apfpksftvbc lv_wfpekquugkgqbxa Name lv_qtqpcbaiwbdxlh lv_uhcumtyikxc lv_tfgmxddnscr "
      "ModifiedOn lv_bokocf lv_geljlppn lv_hbzkftcv lv_kijsuocbeqr lv_bnsevdaizy "
      "lv_oqbsubxihbyxrxk lv_vuiqunkkqoz lv_rmefbtbibmxnb lv_dwjjhjzwcsl lv_sbtbwjnhrphyjva "
      "lv_zlbgeiqc lv_sroknmopiq lv_autaxqpaguhzq LIMIT 0 50";

  const int num_iterations = 5;
  std::vector<int64_t> parse_times_us;
  parse_times_us.reserve(num_iterations);

  // Run the same query multiple times and measure each execution
  for (int i = 0; i < num_iterations; i++) {
    absl::Time start = absl::Now();

    {
      QueryDriver driver{};
      driver.ResetScanner();
      driver.SetParams(&params);
      driver.SetInput(std::string{production_query});
      (void)Parser (&driver)();
    }

    absl::Duration elapsed = absl::Now() - start;
    int64_t elapsed_us = absl::ToInt64Microseconds(elapsed);
    parse_times_us.push_back(elapsed_us);

    std::cout << "Iteration " << (i + 1) << ": SearchAlgorithm::Init took " << elapsed_us << " μs"
              << (i == 0 ? " [FIRST - SLOW]" : " [SUBSEQUENT - FAST]") << std::endl;
  }

  // Analyze the performance difference
  if (parse_times_us.size() >= 2) {
    int64_t first_time = parse_times_us[0];
    int64_t min_subsequent_time =
        *std::min_element(parse_times_us.begin() + 1, parse_times_us.end());
    int64_t avg_subsequent_time = 0;
    for (size_t i = 1; i < parse_times_us.size(); i++) {
      avg_subsequent_time += parse_times_us[i];
    }
    avg_subsequent_time /= (parse_times_us.size() - 1);

    double performance_ratio =
        static_cast<double>(first_time) / static_cast<double>(avg_subsequent_time);

    std::cout << "Performance analysis:  " << std::endl;
    std::cout << "First query time:     " << first_time << " μs" << std::endl;
    std::cout << "Min subsequent time:  " << min_subsequent_time << " μs" << std::endl;
    std::cout << "Avg subsequent time:  " << avg_subsequent_time << " μs" << std::endl;
    std::cout << "Performance ratio:    " << std::fixed << std::setprecision(1) << performance_ratio
              << "x" << std::endl;
    std::cout << "Difference:           " << (first_time - avg_subsequent_time) << " μs"
              << std::endl;

    // The test demonstrates the issue - first call should be significantly slower
    if (performance_ratio > 10.0) {
      std::cout << "!!! Significant performance penalty detected !!!: " << performance_ratio
                << "x slower on first query!" << std::endl;
    } else if (performance_ratio > 2.0) {
      std::cout << "Moderate performance difference detected: " << performance_ratio << "x"
                << std::endl;
    } else {
      std::cout << "Performance is consistent between first and subsequent queries" << std::endl;
    }
  }
}

}  // namespace dfly::search
