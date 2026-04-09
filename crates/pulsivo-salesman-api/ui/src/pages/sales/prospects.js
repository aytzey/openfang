'use strict';

export const salesProspectMixins = {
    selectedScoreBars() {
      var score = this.selectedDossierScore;
      if (!score) return [];
      return [
        { key: 'fit', label: 'Fit', value: Number(score.fit_score || 0), risk: false },
        { key: 'intent', label: 'Intent', value: Number(score.intent_score || 0), risk: false },
        { key: 'reach', label: 'Reach', value: Number(score.reachability_score || 0), risk: false },
        { key: 'deliverability', label: 'Deliverability Risk', value: Number(score.deliverability_risk || 0), risk: true },
        { key: 'compliance', label: 'Compliance Risk', value: Number(score.compliance_risk || 0), risk: true }
      ];
    },
    sendGateLabel() {
      var gate = this.selectedDossierScore && this.selectedDossierScore.send_gate;
      if (!gate || !gate.decision) return '-';
      return gate.decision.replace(/_/g, ' ');
    },

    sendGateReason() {
      var gate = this.selectedDossierScore && this.selectedDossierScore.send_gate;
      if (!gate || !gate.decision) return '';
      if (gate.reason) return gate.reason;
      if (Array.isArray(gate.missing)) return gate.missing.join('; ');
      return '';
    },
    prospectActionSuggestion(p) {
      if (!p) return '';
      var status = p.profile_status || '';
      var hasEmail = !!(p.primary_email);
      var hasLinkedin = !!(p.primary_linkedin_url);
      if (this.isB2C) {
        var fit = Number(p.fit_score || 0);
        if (status === 'contact_ready' && hasLinkedin && fit >= 78) return 'DM + teklif ac';
        if (status === 'contact_ready' && hasLinkedin) return 'Hook netlestir';
        if (hasLinkedin) return 'Proof zenginlestir';
        return 'Discovery genislet';
      }
      if (status === 'contact_ready' && hasEmail) return 'Send email now';
      if (status === 'contact_ready' && hasLinkedin) return 'LinkedIn outreach';
      if (status === 'email_only') return 'Search LinkedIn';
      if (status === 'contact_identified') return 'Verify contact';
      if (status === 'company_only') return 'Research needed';
      return '';
    },

    prospectActionClass(p) {
      if (!p) return 'badge-muted';
      var status = p.profile_status || '';
      if (status === 'contact_ready') return 'badge-success';
      if (status === 'email_only' || status === 'contact_identified') return 'badge-warn';
      return 'badge-muted';
    },

    prospectTechStack(p) {
      if (!p || !Array.isArray(p.tech_stack) || p.tech_stack.length === 0) return '';
      return p.tech_stack.join(', ');
    },
    leadReasonsPreview(lead) {
      if (!lead || !Array.isArray(lead.reasons)) return [];
      return lead.reasons.slice(0, 2);
    },

    prospectSignalsPreview(prospect) {
      if (!prospect || !Array.isArray(prospect.matched_signals)) return [];
      return prospect.matched_signals.slice(0, 3);
    },

    prospectPrimaryContact(prospect) {
      if (!prospect) return '-';
      var name = prospect.primary_contact_name || '';
      var title = prospect.primary_contact_title || '';
      if (!name && this.isB2C && prospect.primary_linkedin_url) {
        var match = String(prospect.primary_linkedin_url).match(/(?:instagram\.com|tiktok\.com)\/@?([^/?#]+)/i);
        name = match && match[1] ? ('@' + String(match[1]).replace(/^@/, '')) : 'Social profile';
      }
      if (!name) {
        name = this.isB2C ? 'Profile bulundu' : 'Temas yok';
      }
      return title ? (name + ' / ' + title) : name;
    },

    prospectChannels(prospect) {
      if (!prospect) return '-';
      var channels = [];
      if (prospect.primary_email) channels.push('email');
      if (prospect.primary_linkedin_url) {
        if (this.isB2C && /instagram\.com|tiktok\.com/i.test(prospect.primary_linkedin_url)) {
          if (/instagram\.com/i.test(prospect.primary_linkedin_url)) channels.push('instagram_dm');
          if (/tiktok\.com/i.test(prospect.primary_linkedin_url)) channels.push('tiktok_dm');
        } else {
          channels.push('linkedin_assist');
        }
      }
      return channels.length ? channels.join(' + ') : 'sirket seviyesi';
    },

    prospectNextAction(prospect) {
      if (!prospect) return '-';
      var score = typeof prospect.fit_score === 'number' ? prospect.fit_score : 50;
      var isHigh = score >= 75;
      if (this.isB2C) {
        if (prospect.primary_linkedin_url) {
          return isHigh ? '2 satirlik opener + yumusak CTA hazirla' : 'Hook, proof ve lokal cue zenginlestir';
        }
        return 'Ek social profile veya lokal pazar sinyali ara';
      }
      if (prospect.profile_status === 'contact_ready') {
        if (prospect.primary_email && prospect.primary_linkedin_url) {
          return isHigh ? 'Hemen email gonder + LinkedIn follow-up planla' : 'Email ile basla, LinkedIn follow-up';
        }
        if (prospect.primary_email) {
          return isHigh ? 'Hemen email taslagini onaya gonder' : 'Email taslagini onaya gonder';
        }
        if (prospect.primary_linkedin_url) return 'LinkedIn operator-assist gorevi ac';
        return 'Email pattern tahmini dene veya LinkedIn ara';
      }
      if (prospect.profile_status === 'email_only') return 'LinkedIn profilini ara, kisisel email bul';
      if (prospect.profile_status === 'contact_identified') return "Kanal dogrulama yap ve lead'e yuksel";
      return 'Arastirma gerekli: buying committee ve temas kanali cikar';
    },

    prospectOsintLinks(prospect) {
      if (!prospect) return [];
      var links = [];
      if (prospect.primary_linkedin_url) links.push(prospect.primary_linkedin_url);
      if (prospect.company_linkedin_url) links.push(prospect.company_linkedin_url);
      if (Array.isArray(prospect.osint_links)) {
        links = links.concat(prospect.osint_links);
      }
      var seen = new Set();
      return links
        .filter(function(link) { return typeof link === 'string' && link.trim().length > 0; })
        .map(function(link) { return link.trim(); })
        .filter(function(link) {
          if (seen.has(link)) return false;
          seen.add(link);
          return true;
        })
        .slice(0, 6);
    },

    prospectStatusClass(status) {
      if (this.isB2C && status === 'contact_ready') return 'badge-success';
      if (status === 'contact_ready') return 'badge-success';
      if (status === 'contact_identified') return 'badge-warn';
      return 'badge-muted';
    },

    prospectResearchClass(status) {
      return status === 'llm_enriched' || status === 'social_enriched' ? 'badge-success' : 'badge-muted';
    },

    prospectConfidencePct(prospect) {
      if (!prospect || typeof prospect.research_confidence !== 'number') return '0%';
      return String(Math.round(prospect.research_confidence * 100)) + '%';
    },

    async selectProspect(prospect) {
      if (!prospect || !prospect.id) return;
      this.selectedProspectId = prospect.id;
      await this.loadSelectedDossier();
    },

    ensureProspectSelection() {
      var current = this.selectedProspectRecord();
      if (current && current.id) {
        this.selectedProspectId = current.id;
        return;
      }
      if (this.runProspects.length > 0) {
        this.selectedProspectId = this.runProspects[0].id;
        return;
      }
      if (this.prospects.length > 0) {
        this.selectedProspectId = this.prospects[0].id;
        return;
      }
      this.selectedProspectId = '';
    },
};
