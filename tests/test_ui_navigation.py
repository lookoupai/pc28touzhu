"""Route and shared-navigation contracts. Uses an in-memory fake repository."""
import re
import unittest
from html.parser import HTMLParser
from pc28touzhu.api.app import PlatformApiApplication, build_testing_environ
from pc28touzhu.api.ui_navigation import PAGE_ROUTES
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from tests.test_api import FakeRepository


class Markup(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.ids = []
        self.selected = []
        self.navigation = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if attrs.get("id"):
            self.ids.append(attrs["id"])
        if tag == "a" and "app-nav-link" in attrs.get("class", ""):
            self.navigation.append(attrs["href"])
            if attrs.get("aria-current") == "page":
                self.selected.append(attrs["href"])


class UiNavigationTests(unittest.TestCase):
    def setUp(self):
        self.repo = FakeRepository()
        self.regular = self.repo.create_user_record(username="navigation-user", role="user", status="active")
        self.app = PlatformApiApplication(self.repo, "test", "navigation-test")

    def get(self, path, uid=None, query="", method="GET"):
        captured = {}
        headers = {"HTTP_COOKIE": "%s=%s" % (SESSION_COOKIE_NAME, build_session_cookie_value(uid, "navigation-test"))} if uid else {}
        def start_response(status, response_headers):
            captured.update(status=status, headers=dict(response_headers))
        body = b"".join(self.app(build_testing_environ(path, query=query, method=method, headers=headers), start_response)).decode()
        return captured, body

    def test_every_page_has_one_shared_menu_and_correct_current_item(self):
        for path, page in PAGE_ROUTES.items():
            with self.subTest(path=path):
                response, body = self.get(path, uid=1)
                self.assertEqual(response["status"], "200 OK")
                parsed = Markup(body)
                self.assertEqual(parsed.selected, [path])
                self.assertEqual(parsed.navigation, list(PAGE_ROUTES))
                self.assertEqual(len(parsed.ids), len(set(parsed.ids)), "duplicate DOM ids")
                self.assertNotRegex(body, r"__[A-Z_]+__")
                self.assertEqual(len(re.findall(r'<script src="/assets/app-shell.js\?v=\d+"', body)), 1)
                self.assertIn('class="has-app-shell"', body)

    def test_workspace_pages_include_only_their_own_forms(self):
        forms = {"sources": "sourceForm", "accounts": "accountForm", "targets": "targetForm", "templates": "messageTemplateForm", "subscriptions": "subscriptionForm"}
        for scope, expected in forms.items():
            with self.subTest(scope=scope):
                _, body = self.get("/autobet/" + scope, uid=1)
                ids = Markup(body).ids
                self.assertIn(expected, ids)
                for other in set(forms.values()) - {expected}:
                    self.assertNotIn(other, ids)
                self.assertNotIn("botBindingSection", ids)
                self.assertNotIn("autobetOverviewSection", ids)

    def test_trailing_slash_normalization_keeps_query_for_all_pages(self):
        for path in PAGE_ROUTES:
            if path == "/":
                continue
            response, _ = self.get(path + "/", query="status=failed&signal_id=12")
            self.assertEqual(response["status"], "302 Found")
            self.assertEqual(response["headers"]["Location"], path + "?status=failed&signal_id=12")
        response, _ = self.get("/autobet/", query="stat_date=2026-11-08")
        self.assertEqual(response["headers"]["Location"], "/?stat_date=2026-11-08")

    def test_admin_page_permissions_and_denied_page_login_shell(self):
        for path, page in PAGE_ROUTES.items():
            if not page["admin"]:
                continue
            for uid, expected in ((None, "401 Unauthorized"), (self.regular["id"], "403 Forbidden"), (1, "200 OK")):
                with self.subTest(path=path, uid=uid):
                    response, body = self.get(path, uid=uid)
                    self.assertEqual(response["status"], expected)
                    if uid != 1:
                        self.assertIn('data-admin-access-denied', body)
                        self.assertIn('id="accountDialog"', body)
                        self.assertIn('id="appNavigationData"', body)
                        self.assertNotIn('id="adminContent"', body)

    def test_page_posts_and_unknown_routes_are_rejected(self):
        for path in PAGE_ROUTES:
            response, _ = self.get(path, method="POST", uid=1)
            self.assertEqual(response["status"], "405 Method Not Allowed")
        response, _ = self.get("/autobet/unknown")
        self.assertEqual(response["status"], "404 Not Found")


if __name__ == "__main__":
    unittest.main()
