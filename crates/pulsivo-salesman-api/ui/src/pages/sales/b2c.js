'use strict';

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function nonEmpty(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function truncateText(value, maxLen) {
  var text = nonEmpty(value).replace(/\s+/g, ' ');
  if (!text || text.length <= maxLen) return text;
  return text.slice(0, Math.max(0, maxLen - 3)).trim() + '...';
}

function firstSentence(value, fallback) {
  var text = nonEmpty(value);
  if (!text) return fallback || '';
  var match = text.match(/[^.!?]+[.!?]?/);
  return truncateText(match && match[0] ? match[0] : text, 140);
}

function platformKeyFromUrl(url) {
  var value = nonEmpty(url).toLowerCase();
  if (value.indexOf('instagram.com') >= 0) return 'instagram';
  if (value.indexOf('tiktok.com') >= 0) return 'tiktok';
  if (value.indexOf('linkedin.com') >= 0) return 'linkedin';
  if (value.indexOf('@') === 0) return 'social';
  return 'web';
}

function platformLabelFromKey(key) {
  if (key === 'instagram') return 'Instagram';
  if (key === 'tiktok') return 'TikTok';
  if (key === 'linkedin') return 'LinkedIn';
  if (key === 'social') return 'Social';
  return 'Web';
}

function cleanSignal(signal) {
  var text = nonEmpty(signal);
  if (!text) return '';
  return text
    .replace(/^Consumer niche match:\s*/i, '')
    .replace(/^Locality hint:\s*/i, '')
    .replace(/^Local market focus:\s*/i, '')
    .replace(/^Local market signal:\s*/i, '')
    .replace(/^Kamuya acik sinyal:\s*/i, '')
    .replace(/^Public social activity suggests\s*/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractSignalValue(prospect, prefixes) {
  var signals = asArray(prospect && prospect.matched_signals);
  for (var i = 0; i < signals.length; i++) {
    var raw = nonEmpty(signals[i]);
    var lower = raw.toLowerCase();
    for (var j = 0; j < prefixes.length; j++) {
      if (lower.indexOf(prefixes[j]) === 0) {
        var idx = raw.indexOf(':');
        return cleanSignal(idx >= 0 ? raw.slice(idx + 1) : raw);
      }
    }
  }
  return '';
}

function topKeywords(values, limit) {
  var stopwords = {
    local: true,
    market: true,
    signal: true,
    focus: true,
    consumer: true,
    profile: true,
    public: true,
    social: true,
    discovered: true,
    via: true,
    active: true,
    interest: true,
    hints: true,
    hint: true,
    match: true,
    niche: true,
    bio: true,
    trend: true,
    signals: true,
    and: true,
    the: true,
    for: true,
    ile: true,
    icin: true,
    gibi: true,
    olan: true,
    veya: true,
    this: true,
    that: true
  };
  var counts = {};
  var entries = asArray(values);
  for (var i = 0; i < entries.length; i++) {
    var text = cleanSignal(entries[i]).toLowerCase();
    if (!text) continue;
    var parts = text.split(/[^a-z0-9]+/);
    for (var j = 0; j < parts.length; j++) {
      var token = parts[j];
      if (!token || token.length < 3 || stopwords[token]) continue;
      counts[token] = (counts[token] || 0) + 1;
    }
  }
  return Object.keys(counts)
    .sort(function(left, right) {
      return counts[right] - counts[left] || left.localeCompare(right);
    })
    .slice(0, limit || 6)
    .map(function(token) {
      return token.charAt(0).toUpperCase() + token.slice(1);
    });
}

function sortProspects(pool) {
  return pool.sort(function(left, right) {
    var fitDiff = Number(right && right.fit_score ? right.fit_score : 0) - Number(left && left.fit_score ? left.fit_score : 0);
    if (fitDiff) return fitDiff;
    var confidenceDiff = Number(right && right.research_confidence ? right.research_confidence : 0) - Number(left && left.research_confidence ? left.research_confidence : 0);
    if (confidenceDiff) return confidenceDiff;
    return Number(right && right.source_count ? right.source_count : 0) - Number(left && left.source_count ? left && left.source_count : 0);
  });
}

export const salesB2cMixins = {
    b2cProspectPool() {
      var pool = asArray(this.runProspects).length ? this.runProspects.slice() : asArray(this.prospects).slice();
      return sortProspects(pool);
    },

    b2cPlatform(prospect) {
      var key = platformKeyFromUrl(prospect && prospect.primary_linkedin_url);
      return platformLabelFromKey(key);
    },

    b2cPrimaryChannelLabel() {
      var breakdown = this.b2cPlatformBreakdown(1);
      return breakdown.length ? breakdown[0].label : 'Social DM';
    },

    b2cMarketLabel() {
      var industry = nonEmpty(this.profile && this.profile.target_industry) || 'Consumer niche';
      var geo = nonEmpty(this.profile && this.profile.target_geo) || 'LOCAL';
      return industry + ' / ' + geo;
    },

    b2cHighIntentCount() {
      var pool = this.b2cProspectPool();
      var count = 0;
      for (var i = 0; i < pool.length; i++) {
        var fit = Number(pool[i] && pool[i].fit_score ? pool[i].fit_score : 0);
        var confidence = Number(pool[i] && pool[i].research_confidence ? pool[i].research_confidence : 0);
        if (fit >= 74 || (fit >= 68 && confidence >= 0.62)) count += 1;
      }
      return count;
    },

    b2cResearchBacklogCount() {
      var pool = this.b2cProspectPool();
      var count = 0;
      for (var i = 0; i < pool.length; i++) {
        var fit = Number(pool[i] && pool[i].fit_score ? pool[i].fit_score : 0);
        var confidence = Number(pool[i] && pool[i].research_confidence ? pool[i].research_confidence : 0);
        if (fit < 74 || confidence < 0.62) count += 1;
      }
      return count;
    },

    b2cReadinessStage() {
      if (!this.onboarding || !this.onboarding.profile_ready) return 'Setup';
      var count = this.b2cProspectPool().length;
      if (count >= 24) return 'Scale-ready';
      if (count >= 10) return 'Offer-ready';
      if (count >= 1) return 'First traction';
      return 'Discovery';
    },

    b2cReadinessNote() {
      var count = this.b2cProspectPool().length;
      if (!count) {
        return 'Brief netse sistem sosyal profilleri ve lokal sinyalleri toplamaya baslar. Bir sonraki hedef ilk 5 buyer pocketi cikarmak.';
      }
      if (count < 10) {
        return 'Havuz olusuyor. Simdi en iyi 5 profile icin hook, proof ve CTA formatini standartlastirin.';
      }
      if (count < 24) {
        return 'Yeterli buyer pocket var. Simdi teklif, yaratıcı ve DM scripti test edilip cevaba gore ayrismali.';
      }
      return 'Pipeline dolu. Bundan sonraki kaldirac: daha keskin teklif, daha iyi proof ve kanal bazli varyasyon.';
    },

    b2cGrowthNarrative() {
      var product = nonEmpty(this.profile && this.profile.product_name) || 'Marka';
      var market = this.b2cMarketLabel();
      var count = this.b2cProspectPool().length;
      var platform = this.b2cPrimaryChannelLabel();
      var locality = this.b2cLocalityBreakdown(1);
      var localityText = locality.length ? locality[0].label : (nonEmpty(this.profile && this.profile.target_geo) || 'lokal cep');
      if (!count) {
        return product + ' icin ' + market + ' hedefi hazir. Sistem Instagram, TikTok ve lokal baglam sinyallerinden buyer pool cikarmaya odaklanacak.';
      }
      return product + ' icin ' + market + ' ekseninde ' + String(count) + ' public buyer/profile bulundu. En guclu temas kanali ' + platform + ', en belirgin market pocket ise ' + localityText + '. Simdi generik tanitim degil, gorunen niyet + lokal baglam + yumusak teklif kombinasyonuyla cevap alinmali.';
    },

    b2cPlatformBreakdown(limit) {
      var pool = this.b2cProspectPool();
      var meta = {
        instagram: { label: 'Instagram', note: 'Gorsel zevk, rutin ve stil sinyali' },
        tiktok: { label: 'TikTok', note: 'Trend, hiz ve kolay tukenebilir format' },
        linkedin: { label: 'LinkedIn', note: 'Operator veya creator-professional kesisimi' },
        web: { label: 'Web', note: 'Sosyal kanaldan zayif, ek enrichment ister' }
      };
      var counts = {};
      for (var i = 0; i < pool.length; i++) {
        var key = platformKeyFromUrl(pool[i] && pool[i].primary_linkedin_url);
        counts[key] = (counts[key] || 0) + 1;
      }
      return Object.keys(counts)
        .sort(function(left, right) {
          return counts[right] - counts[left];
        })
        .slice(0, limit || 4)
        .map(function(key) {
          var info = meta[key] || meta.web;
          return {
            key: key,
            label: info.label,
            count: counts[key],
            note: info.note
          };
        });
    },

    b2cLocalityBreakdown(limit) {
      var pool = this.b2cProspectPool();
      var counts = {};
      for (var i = 0; i < pool.length; i++) {
        var locality = this.b2cProfileLocality(pool[i]);
        counts[locality] = (counts[locality] || 0) + 1;
      }
      return Object.keys(counts)
        .sort(function(left, right) {
          return counts[right] - counts[left] || left.localeCompare(right);
        })
        .slice(0, limit || 4)
        .map(function(label) {
          return { label: label, count: counts[label] };
        });
    },

    b2cTopSignalTags(limit) {
      var pool = this.b2cProspectPool();
      var signalBag = [];
      for (var i = 0; i < pool.length; i++) {
        signalBag = signalBag.concat(asArray(pool[i] && pool[i].matched_signals));
      }
      signalBag.push(nonEmpty(this.profile && this.profile.target_industry));
      signalBag.push(nonEmpty(this.profile && this.profile.product_name));
      return topKeywords(signalBag, limit || 6);
    },

    b2cMessagePillars() {
      var product = nonEmpty(this.profile && this.profile.product_name) || 'Marka';
      var description = firstSentence(nonEmpty(this.profile && this.profile.product_description), product + ' icin kisa ve sonuc odakli bir fayda cumlesi netlestirilmeli.');
      var platform = this.b2cPrimaryChannelLabel();
      var locality = this.b2cLocalityBreakdown(1);
      var localityText = locality.length ? locality[0].label : (nonEmpty(this.profile && this.profile.target_geo) || 'lokal market');
      return [
        {
          label: 'Positioning',
          body: description
        },
        {
          label: 'Personalization',
          body: platform + ' temasinda ilk cumle, profilin gorunen niche veya ' + localityText + ' baglamindan acilmali. Mesaj urun dumpi gibi baslamamali.'
        },
        {
          label: 'Conversion',
          body: 'Hedef ilk mesajda satis kapatmak degil; cevap almak, mini teklif istemek ve ikinci mesaja izin cikarmak olmali.'
        }
      ];
    },

    b2cOfferAngles() {
      var product = nonEmpty(this.profile && this.profile.product_name) || 'marka';
      var tags = this.b2cTopSignalTags(3).join(', ');
      var geo = nonEmpty(this.profile && this.profile.target_geo) || 'lokal';
      return [
        {
          label: 'Visible taste match',
          body: product + ' teklifini profilde gorunen estetik, rutin veya ilgi ipucuna bagla. Gerekirse ' + tags + ' gibi sinyallerden ac.'
        },
        {
          label: 'Local proof',
          body: geo + ' veya sehir baglamini one cikar. Lokal teslimat, lokal topluluk veya ayni sehir hissi B2C cevap oranini yukseltir.'
        },
        {
          label: 'Low-friction CTA',
          body: 'DM sonunda agir call yerine yumusak bir izin sorusu kullan: "Uygunsa 2 cumlelik teklif / demo / kod gondereyim mi?"'
        }
      ];
    },

    b2cPriorityProfiles(limit) {
      var pool = this.b2cProspectPool().slice(0, limit || 6);
      var self = this;
      return pool.map(function(prospect) {
        var script = self.b2cScriptFor(prospect);
        return {
          prospect: prospect,
          platform: self.b2cPlatform(prospect),
          locality: self.b2cProfileLocality(prospect),
          why_now: self.b2cProfileWhyNow(prospect),
          opener: script.opener,
          action: self.prospectNextAction(prospect)
        };
      });
    },

    b2cProfileLocality(prospect) {
      return extractSignalValue(prospect, ['locality hint:', 'local market focus:', 'local market signal:']) ||
        nonEmpty(this.profile && this.profile.target_geo) ||
        'Local';
    },

    b2cProfileWhyNow(prospect) {
      var hook = cleanSignal(extractSignalValue(prospect, ['consumer niche match:', 'locality hint:', 'local market focus:', 'local market signal:']));
      if (!hook) {
        var signals = asArray(prospect && prospect.matched_signals);
        hook = cleanSignal(signals.length ? signals[0] : '');
      }
      if (!hook) {
        hook = this.b2cPlatform(prospect) + ' uzerinde aktif gorunum';
      }
      return truncateText('Bu profile simdi gidilmeli cunku gorunen hook net: ' + hook + '.', 160);
    },

    b2cProfileOffer(prospect) {
      var platform = this.b2cPlatform(prospect);
      var product = nonEmpty(this.profile && this.profile.product_name) || 'teklif';
      if (platform === 'TikTok') {
        return product + ' icin hizli demo, trend uyumu ve kolay deneme odakli aci kullan.';
      }
      if (platform === 'Instagram') {
        return product + ' icin estetik uyum, rutin katkisi ve gorsel sonuc odakli aci kullan.';
      }
      return product + ' icin kisa fayda + yumusak izin CTA kombinasyonu kullan.';
    },

    b2cProfileRiskFlags(prospect) {
      var warnings = [];
      var fit = Number(prospect && prospect.fit_score ? prospect.fit_score : 0);
      var confidence = Number(prospect && prospect.research_confidence ? prospect.research_confidence : 0);
      if (fit < 70) warnings.push('Fit orta seviye, mesaji daralt');
      if (confidence < 0.62) warnings.push('Proof az, ikinci kaynaga ihtiyac var');
      if (Number(prospect && prospect.source_count ? prospect.source_count : 0) <= 1) warnings.push('Tek kaynaktan geldi');
      if (!this.b2cProfileLocality(prospect) || this.b2cProfileLocality(prospect) === 'Local') warnings.push('Lokal cue zayif');
      return warnings.slice(0, 3);
    },

    b2cProfileRiskLabel(prospect) {
      var flags = this.b2cProfileRiskFlags(prospect);
      return flags.length ? flags.join(' | ') : 'Risk dusuk. Hook, proof ve CTA ayni eksende.';
    },

    b2cScriptFor(prospect) {
      var display = this.prospectPrimaryContact(prospect).split(' / ')[0];
      if (!display || display === '-' || display === 'Profile bulundu') {
        display = nonEmpty(prospect && prospect.company) || 'orada';
      }
      var hook = cleanSignal(extractSignalValue(prospect, ['consumer niche match:', 'locality hint:', 'local market focus:', 'local market signal:']));
      if (!hook) {
        var signals = asArray(prospect && prospect.matched_signals);
        hook = cleanSignal(signals.length ? signals[0] : '');
      }
      if (!hook) hook = 'gorunen niche ve profil dili';
      var product = nonEmpty(this.profile && this.profile.product_name) || 'teklifimiz';
      var promise = firstSentence(nonEmpty(this.profile && this.profile.product_description), product + ' ile sonuc odakli bir deger onerisi');
      var locality = this.b2cProfileLocality(prospect);
      return {
        opener: truncateText('Selam ' + display + ', profildeki ' + hook + ' ve ' + locality + ' baglami direkt dikkat cekti.', 150),
        pitch: truncateText(product + ' tarafinda one cikan cumle su olmali: ' + promise, 170),
        proof: truncateText('Mesajin ispat noktasi profilin gorunen sinyalleri olmali; ilk temas neden sana yazdigimizi net gostermeli.', 150),
        cta: 'Uygunsa 2 cumlelik mini teklif / demo / kod gondereyim mi?'
      };
    },

    selectedB2CScript() {
      return this.b2cScriptFor(this.selectedProspectRecord());
    },

    b2cRunActionValue(run) {
      return Number(run && run.discovered ? run.discovered : 0);
    }
};
